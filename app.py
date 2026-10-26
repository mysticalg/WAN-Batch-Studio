import copy, json, os, pathlib, queue, subprocess, threading, time, urllib.request, urllib.parse, uuid, sys, shutil
import tkinter as tk
from fractions import Fraction
import tempfile
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
import math
from collections import OrderedDict
import errno, traceback, http.client, urllib.error
from tkinter import ttk, filedialog, messagebox

VERSION = '1.0.0'
# Installed releases keep data outside the application/uninstaller directory.
ROOT = pathlib.Path(os.environ.get('WAN_BATCH_STUDIO_DATA') or (str(pathlib.Path(os.environ.get('LOCALAPPDATA',str(pathlib.Path.home()))) / 'WANBatchStudio') if getattr(sys,'frozen',False) else str(pathlib.Path(__file__).resolve().parent)))
ROOT.mkdir(parents=True,exist_ok=True)
STATE = ROOT / 'queue.json'
MEDIA_CONTEXT=threading.local()
SCRIPT_LOCK=threading.RLock()
LONDON=ZoneInfo('Europe/London')

class Cancelled(Exception): pass
class PermanentFailure(RuntimeError): pass
class AmbiguousSubmission(PermanentFailure): pass
class PromptMissing(PermanentFailure): pass
RETRY_DELAYS=(2,5,15,30,60)

def temporary_error(error):
    if isinstance(error,PromptMissing): return True
    if isinstance(error,urllib.error.HTTPError): return error.code in (408,429,500,502,503,504)
    if isinstance(error,urllib.error.URLError): return True
    if isinstance(error,subprocess.CalledProcessError):
        stderr=error.stderr.decode('utf-8',errors='replace') if isinstance(error.stderr,bytes) else str(error.stderr or '')
        return any(marker in stderr.lower() for marker in ('permission denied','resource temporarily unavailable','sharing violation','used by another process','input/output error'))
    if isinstance(error,(TimeoutError,ConnectionError,http.client.IncompleteRead,http.client.RemoteDisconnected,json.JSONDecodeError)): return True
    if isinstance(error,OSError):
        return error.errno in (errno.EAGAIN,errno.EBUSY,errno.EINTR,errno.ETIMEDOUT,errno.ECONNRESET,errno.ECONNREFUSED,errno.ENETUNREACH,errno.EHOSTUNREACH) or getattr(error,'winerror',None) in (5,32,33,53,64,121,1231)
    return False

def error_summary(error):
    # Never include exception bodies, HTTP response bodies, command arguments or prompts.
    name=type(error).__name__
    if isinstance(error,urllib.error.HTTPError): return f'HTTP {error.code}'
    if isinstance(error,subprocess.CalledProcessError): return f'{name}: process exit {error.returncode}'
    if isinstance(error,PermanentFailure): return str(error)
    code=getattr(error,'winerror',None) or getattr(error,'errno',None)
    return name+(f' (system code {code})' if code is not None else '')

def recovery_summary(state):
    record=state.get('recovery',{})
    if record.get('status')=='retrying': return f'Retrying {record["phase"]}: attempt {record["attempt"]}/{record["limit"]}; next try in {record["delay"]}s. {record["reason"]}'
    if record.get('status')=='stopped': return f'Stopped at {record["phase"]}: {record["reason"]}. Tracking retained; inspect logs, then Start / Resume.'
    if record.get('status')=='recovered': return f'Recovered {record["phase"]} after {record["attempt"]} attempts.'
    if record.get('status')=='attention': return f'Attention at {record["phase"]}: {record["reason"]}. Completed segments retained; retry joins separately.'
    if state.get('worker_state',{}).get('status')=='paused': return 'Batch paused after the current segment. Start / Resume continues the queue.'
    if state.get('worker_state',{}).get('status')=='completed': return 'Batch completed. Diagnostic history is saved locally.'
    return 'Recovery: ready. Diagnostic logs are saved locally.'

def record_render_event(job, kind, data, now=None):
    if data.get('prompt_id')!=job.get('prompt_id',job.get('requested_prompt_id')): return
    now=time.time() if now is None else now
    timing=job.setdefault('render_timing',{})
    stamp=data.get('timestamp')
    if kind=='execution_start':
        timing.update(start_wall=now,source='execution_start event',phase='executing')
        if isinstance(stamp,(int,float)): timing['server_start']=stamp/1000
        timing.pop('seconds',None)
    elif kind in ('execution_success','execution_error','execution_interrupted'):
        timing.update(phase='complete' if kind=='execution_success' else ('cancelled' if kind=='execution_interrupted' else 'failed'),end_wall=now)
        timing['end_kind']=kind
        if isinstance(stamp,(int,float)): timing['server_end']=stamp/1000
        if isinstance(stamp,(int,float)) and timing.get('server_start') is not None:
            timing['seconds']=max(0,stamp/1000-timing['server_start']); timing['duration_source']='server events'
        elif timing.get('start_wall') is not None:
            timing['seconds']=max(0,now-timing['start_wall']); timing['duration_source']='observed'

def record_render_history(job, history):
    messages=history.get('status',{}).get('messages',[])
    # Authoritative server timestamps recover durations even if no websocket was connected.
    for kind,data in messages:
        if kind not in ('execution_start','execution_success','execution_error','execution_interrupted'): continue
        if not isinstance(data,dict): continue
        stamp=data.get('timestamp')
        record_render_event(job,kind,data,now=stamp/1000 if isinstance(stamp,(int,float)) else time.time())
    timing=job.setdefault('render_timing',{})
    if history.get('status',{}).get('completed') or history.get('status',{}).get('status_str')=='error':
        if timing.get('phase') not in ('complete','failed','cancelled'):
            timing['phase']='failed' if history.get('status',{}).get('status_str')=='error' else 'complete'
            # No terminal timestamp: do not count download/post-processing as render time.
            if timing.get('start_wall') is not None:
                timing['seconds']=max(0,time.time()-timing['start_wall']); timing['duration_source']='observed completion'

def observe_render_queue(job, snapshot, now=None):
    now=time.time() if now is None else now
    timing=job.setdefault('render_timing',{})
    pid=job.get('prompt_id')
    if any(row[1]==pid for row in snapshot['queue_running']):
        timing['phase']='executing'; timing.setdefault('observed_running_at',now)
    elif any(row[1]==pid for row in snapshot['queue_pending']): timing['phase']='queued'

def elapsed_text(job, now=None, verified=True):
    now=time.time() if now is None else now
    timing=job.get('render_timing',{}); phase=timing.get('phase','unverified' if job.get('prompt_id') and job.get('status')=='running' else 'waiting')
    def duration(value):
        seconds=max(0,int(value)); return f'{seconds//3600:02}:{seconds//60%60:02}:{seconds%60:02}'
    prefix=f'Segment {job.get("segment",0)+1} • '
    if phase=='queued': return prefix+'Queued — render timer has not started'
    if phase in ('waiting','submitting'): return prefix+'Waiting to render — timer has not started'
    if phase in ('complete','failed','cancelled'):
        return prefix+phase.title()+' • render '+(duration(timing['seconds'])+(' (approx.)' if timing.get('duration_source')!='server events' else '') if 'seconds' in timing else 'duration unknown')
    label='Elapsed render ' if verified else 'Saved timing; server state unverified • '
    if timing.get('start_wall') is not None: return prefix+label+duration(now-timing['start_wall'])+(' • connection unavailable' if phase=='disconnected' else '')
    if timing.get('observed_running_at') is not None: return prefix+('Server state unverified • ' if not verified else '')+'Render start unknown • observed running for '+duration(now-timing['observed_running_at'])
    return prefix+('Running' if verified else 'Server state unverified')+' • render start unknown'

def london_timestamp(stamp):
    if not isinstance(stamp,(int,float)) or isinstance(stamp,bool) or not math.isfinite(stamp): return 'Unknown'
    try: return datetime.fromtimestamp(stamp,LONDON).strftime('%d %b %Y %H:%M:%S %Z')
    except (ValueError,OverflowError,OSError): return 'Unknown'

def render_dates(job):
    timing=job.get('render_timing',{}); phase=timing.get('phase')
    terminal=phase in ('complete','failed','cancelled') or job.get('status')=='done'
    pending=phase in ('waiting','submitting','queued') or (job.get('status')=='pending' and phase not in ('executing','disconnected','complete','failed','cancelled'))
    start=london_timestamp(timing.get('server_start'))
    if start=='Unknown' and pending: start='Not started'
    end=london_timestamp(timing.get('server_end'))
    if end=='Unknown' and not terminal: end='Not finished' if not pending else '—'
    if phase in ('failed','cancelled'): end+=' ('+phase+')'
    return start,end

def preview_input(jobs, job):
    if job.get('input_path'): return job['input_path']
    if job['segment']==0: return job['source']
    previous=next((j for j in jobs if j.get('group')==job.get('group') and j['segment']==job['segment']-1),None)
    return previous.get('frame') if previous and previous['status']=='done' else None

def load_thumbnail(path, known_signature=None):
    try:
        from PIL import Image, ImageOps
        stat=pathlib.Path(path).stat(); signature=(stat.st_mtime_ns,stat.st_size)
        if known_signature==signature: return None,signature
        with Image.open(path) as source:
            thumb=ImageOps.exif_transpose(source).convert('RGB'); thumb.thumbnail((64,56))
        return thumb,signature
    except Exception: return None,None

class ThumbnailLoader:
    def __init__(self):
        self.requests=queue.Queue(maxsize=32); self.results=queue.Queue(); self.closed=threading.Event()
        for _ in range(2): threading.Thread(target=self.work,daemon=True).start()
    def work(self):
        while not self.closed.is_set():
            try: path,known_signature=self.requests.get(timeout=.5)
            except queue.Empty: continue
            image,signature=load_thumbnail(path,known_signature); self.results.put((path,image,signature))

class PromptEvents:
    """Read-only prompt event listener. Only the queue worker mutates persisted timing."""
    def __init__(self,url,client_id):
        self.events=queue.Queue(); self.stop=threading.Event(); self.socket=None
        try:
            import websocket
            self.socket=websocket.create_connection(url.replace('http://','ws://',1).replace('https://','wss://',1)+'/ws?'+urllib.parse.urlencode({'clientId':client_id}),timeout=2)
            self.socket.settimeout(.5)
            self.thread=threading.Thread(target=self.read,daemon=True); self.thread.start()
        except Exception: self.socket=None
    def read(self):
        while not self.stop.is_set():
            try:
                raw=self.socket.recv()
                if not raw: break
                if isinstance(raw,str): self.events.put((json.loads(raw),time.time()))
            except Exception as e:
                if e.__class__.__name__=='WebSocketTimeoutException': continue
                break
    def drain(self,job):
        while True:
            try: message,received=self.events.get_nowait()
            except queue.Empty: break
            if isinstance(message.get('data'),dict): record_render_event(job,message.get('type'),message['data'],received)
    def close(self):
        self.stop.set()
        if self.socket:
            try: self.socket.close()
            except Exception: pass

def check_cancel():
    event=getattr(MEDIA_CONTEXT,'cancel',None)
    if event is not None and event.is_set(): raise Cancelled('Cancellation requested')

def media_run(args, **kwargs):
    check_cancel()
    with subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0) as process:
        while True:
            try: stdout,stderr=process.communicate(timeout=.2); break
            except subprocess.TimeoutExpired:
                try: check_cancel()
                except Cancelled:
                    process.kill(); process.communicate(); raise
        if process.returncode: raise subprocess.CalledProcessError(process.returncode,args,stdout,stderr)
        check_cancel()
        return stdout

def valid_queue(value):
    return isinstance(value,dict) and isinstance(value.get('jobs'),list) and all(isinstance(job,dict) and all(key in job for key in ('id','status','segment','source')) for job in value['jobs'])

def durable_replace(path, data):
    path=pathlib.Path(path); temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with open(temporary,'wb') as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:
        if temporary.exists(): temporary.unlink()

def atomic(path, value):
    path=pathlib.Path(path)
    with SCRIPT_LOCK:
        data=json.dumps(value,indent=2).encode('utf-8')
        if path.name=='queue.json' and path.exists():
            previous=path.read_bytes()
            try: healthy=valid_queue(json.loads(previous))
            except (ValueError,UnicodeError): healthy=False
            if healthy:
                backup=path.with_name('queue.json.bak')
                if backup.exists():
                    older=backup.read_bytes()
                    try: older_healthy=valid_queue(json.loads(older))
                    except (ValueError,UnicodeError): older_healthy=False
                    if older_healthy: durable_replace(path.with_name('queue.json.bak2'),older)
                durable_replace(backup,previous)
        durable_replace(path,data)

def load_queue_state(path):
    path=pathlib.Path(path)
    if not path.exists(): return {'jobs':[]}
    try:
        state=json.loads(path.read_text(encoding='utf-8-sig'))
        if not valid_queue(state): raise ValueError('Invalid queue structure')
        return state
    except (OSError,ValueError,UnicodeError):
        # Keep the exact damaged bytes before allowing any future save.
        preserved=path.with_name('queue.corrupt.'+str(time.time_ns())+'.json')
        durable_replace(preserved,path.read_bytes())
        for name in ('queue.json.bak','queue.json.bak2'):
            backup=path.with_name(name)
            try:
                state=json.loads(backup.read_text(encoding='utf-8-sig'))
                if not valid_queue(state): continue
                state['startup_notice']='Damaged queue preserved; restored '+name+'. Review before resuming; no render started.'
                return state
            except (OSError,ValueError,UnicodeError): pass
        return {'jobs':[],'startup_notice':'Queue damaged; preserved at '+str(preserved)+'. No valid automatic backup found. Re-add planned work; media files are untouched.',
                'recovery':{'status':'stopped','phase':'startup_queue_recovery','reason':'Damaged queue preserved; no valid automatic backup','time':time.time()}}

class Client:
    def __init__(self, url): self.url = url.rstrip('/')
    def get(self, path):
        with urllib.request.urlopen(self.url + path, timeout=10) as r: return json.load(r)
    def post(self, path, data, content='application/json'):
        if content == 'application/json': data = json.dumps(data).encode()
        with urllib.request.urlopen(urllib.request.Request(self.url + path, data, {'Content-Type': content}), timeout=10 if path in ('/interrupt','/queue') else 30) as r:
            body=r.read(); return json.loads(body) if body else {}
    def targeted_interrupt_supported(self):
        try:
            support=json.loads((ROOT/'cancellation_support.json').read_text())
            return self.url==support['url'] and hashlib.sha256(pathlib.Path(support['server_source']).read_bytes()).hexdigest()==support['sha256']
        except (OSError,ValueError,KeyError): return False
    def upload(self, path):
        boundary = uuid.uuid4().hex
        name = uuid.uuid4().hex + pathlib.Path(path).suffix
        body = ('--'+boundary+'\r\nContent-Disposition: form-data; name="image"; filename="'+name+'"\r\nContent-Type: application/octet-stream\r\n\r\n').encode() + pathlib.Path(path).read_bytes() + ('\r\n--'+boundary+'--\r\n').encode()
        r = self.post('/upload/image', body, 'multipart/form-data; boundary='+boundary)
        return (r.get('subfolder','')+'/' + r['name']).lstrip('/')

def load_prompt(path):
    w = json.loads(pathlib.Path(path).read_text(encoding='utf-8-sig'))
    if 'prompt' in w: w = w['prompt']
    if not w or not all(isinstance(n,dict) and 'class_type' in n for n in w.values()):
        raise ValueError('Import an API-format workflow: in ComfyUI enable Dev mode, then Save (API Format). Visual graph JSON cannot be safely converted automatically.')
    images = [k for k,n in w.items() if n['class_type']=='LoadImage']
    saves = [k for k,n in w.items() if n['class_type'] in ('SaveVideo','VHS_VideoCombine')]
    if len(images)!=1 or len(saves)!=1: raise ValueError('Requires exactly one LoadImage and one SaveVideo/VHS_VideoCombine. Export a dedicated batch workflow.')
    return w, images[0], saves[0]

def describe(w):
    models = [str(v) for n in w.values() for k,v in n['inputs'].items() if 'name' in k and isinstance(v,str)]
    lengths = [n['inputs'].get('length') for n in w.values() if n['class_type']=='WanImageToVideo']
    rates = [n['inputs'].get('fps',n['inputs'].get('frame_rate')) for n in w.values() if n['class_type'] in ('CreateVideo','VHS_VideoCombine')]
    duration = f'{lengths[0]/rates[0]:.2f}s ({lengths[0]} frames / {rates[0]} fps)' if lengths and rates and isinstance(lengths[0],(int,float)) and isinstance(rates[0],(int,float)) else 'Duration cannot be derived from literal inputs'
    return duration+'\n'+'\n'.join(models)

def last_frame(video, frame):
    # Decode every frame in order; update one PNG, leaving the actual final decoded frame.
    media_run(['ffmpeg','-v','error','-xerror','-i',str(video),'-map','0:v:0','-an','-vsync','0','-update','1','-y',str(frame)])
    if not frame.exists() or not frame.stat().st_size: raise RuntimeError('No decoded final frame')

def probe(path):
    return json.loads(media_run(['ffprobe','-v','error','-count_frames','-show_streams','-of','json',str(path)]))['streams']

def join_segments(paths, destination):
    """Keep segment zero; discard decoded frame zero from every later segment."""
    metadata=[probe(p) for p in paths]
    videos=[next(s for s in streams if s['codec_type']=='video') for streams in metadata]
    rate=Fraction(videos[0]['avg_frame_rate'])
    if rate<=0: raise ValueError('Invalid frame rate')
    shape=(videos[0]['width'],videos[0]['height'])
    colour_keys={'color_range':'-color_range','color_space':'-colorspace','color_transfer':'-color_trc','color_primaries':'-color_primaries'}
    colours={key:videos[0].get(key) for key in colour_keys if videos[0].get(key) not in (None,'unknown','unspecified','reserved')}
    source_colours=colours.copy()
    if colours.get('color_space')=='gbr':
        # RGB -> delivery YUV necessarily changes matrix/range; never label YUV as RGB.
        colours.pop('color_space',None); colours.pop('color_range',None)
    colour_args=[part for key,value in colours.items() for part in (colour_keys[key],value)]
    params={'color_range':'range','color_space':'colorspace','color_transfer':'color_trc','color_primaries':'color_primaries'}
    tagfilter='setparams='+':'.join(params[key]+'='+({'tv':'limited','pc':'full'}.get(value,value) if key=='color_range' else value) for key,value in colours.items()) if colours else ''
    for v in videos:
        if (v['width'],v['height'])!=shape or Fraction(v['avg_frame_rate'])!=rate or Fraction(v['r_frame_rate'])!=rate:
            raise ValueError('Joining requires matching constant frame rates and resolution. Source segments are preserved.')
        if any(v.get(key)!=value for key,value in source_colours.items()): raise ValueError('Joining requires matching colour metadata; no automatic colour correction is applied.')
    counts=[int(v['nb_read_frames']) for v in videos]
    if any(n <= (i>0) for i,n in enumerate(counts)): raise ValueError('Segment has no frames after boundary trim')
    expected=sum(counts)-len(paths)+1
    audio=any(any(s['codec_type']=='audio' for s in streams) for streams in metadata)
    destination=pathlib.Path(destination); destination.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='join_',dir=destination.parent) as temp:
        temp=pathlib.Path(temp); normalized=[]
        for i,(path,streams,n) in enumerate(zip(paths,metadata,counts)):
            trim=int(i>0); duration=float(Fraction(n-trim,1)/rate)
            args=['ffmpeg','-v','error','-xerror','-i',str(path)]
            has_audio=any(s['codec_type']=='audio' for s in streams)
            if audio and not has_audio: args+=['-f','lavfi','-i','anullsrc=r=48000:cl=stereo']
            filters=[f'[0:v:0]trim=start_frame={trim},setpts=N/({rate}*TB),setsar=1'+(','+tagfilter if tagfilter else '')+'[v]']
            if audio:
                source='0:a:0' if has_audio else '1:a:0'
                filters.append(f'[{source}]atrim=start={float(Fraction(trim,1)/rate)},asetpts=PTS-STARTPTS,aresample=48000,aformat=channel_layouts=stereo,apad,atrim=duration={duration}[a]')
            part=temp/f'{i}.mkv'; normalized.append(part)
            args+=['-filter_complex',';'.join(filters),'-map','[v]','-c:v','ffv1','-fps_mode','passthrough']+colour_args
            args+=['-map','[a]','-c:a','pcm_s16le'] if audio else ['-an']
            media_run(args+[str(part)])
        listing=temp/'concat.txt'; listing.write_text(''.join(f"file '{p.as_posix()}'\n" for p in normalized),encoding='utf-8')
        result=temp/'joined.mp4'
        media_run(['ffmpeg','-v','error','-xerror','-f','concat','-safe','0','-i',str(listing),'-map','0:v:0']+(['-vf',tagfilter] if tagfilter else [])+['-c:v','libx264','-crf','18','-pix_fmt','yuv420p','-fps_mode','passthrough']+colour_args+(['-map','0:a:0','-c:a','aac'] if audio else ['-an'])+['-movflags','+faststart',str(result)])
        actual=next(s for s in probe(result) if s['codec_type']=='video')
        if int(actual['nb_read_frames'])!=expected: raise RuntimeError('Joined frame count differs from expected count')
        # H.264 may omit the VUI for an otherwise untagged limited-range stream (limited is its default).
        checked=dict(actual)
        if 'color_range' not in checked and str(actual.get('pix_fmt','')).startswith('yuv') and not str(actual.get('pix_fmt','')).startswith('yuvj'): checked['color_range']='tv'
        if any(checked.get(key)!=value for key,value in colours.items()): raise RuntimeError('Joined colour metadata differs from the source; output not committed')
        media_run(['ffmpeg','-v','error','-xerror','-i',str(result),'-f','null','-'])
        os.replace(result,destination)
    return expected

def groups(jobs):
    result={}; current=None
    for j in jobs:
        if 'group' not in j:
            if j['segment']==0 or current is None: current=j['id']
            j['group']=current
        current=j['group']; result.setdefault(current,[]).append(j)
    return result

def segment_input(jobs, job):
    if job['segment']==0: return job['source']
    group=groups(jobs)[job['group']]
    previous=[p for p in group if p['segment']==job['segment']-1]
    if len(previous)!=1 or previous[0]['status']!='done':
        raise RuntimeError('Waiting for the immediately previous segment to complete')
    frame=previous[0].get('frame')
    if not frame or not pathlib.Path(frame).is_file(): raise RuntimeError('Previous segment continuation frame is missing')
    return frame

def remove_pending_groups(state, selected=None):
    """Remove only wholly unsubmitted groups, preserving every tracked prompt and dependency."""
    removable=[]; protected=[]
    for key,jobs in groups(state['jobs']).items():
        if selected is not None and key not in selected: continue
        safe=all(j['status']=='pending' and not j.get('prompt_id') and not j.get('submitted_prompt') for j in jobs)
        (removable if safe else protected).append(key)
    state['jobs']=[j for j in state['jobs'] if j['group'] not in removable]
    return removable,protected

def tracked_work(job):
    return job['status'] in ('running','submitting','cancel_unconfirmed') or (job['status']!='done' and bool(job.get('prompt_id') or job.get('submitted_prompt')))

def hide_list_rows(view, jobs, selected, joins=None):
    grouped=groups(jobs); by_id={j['id']:j for j in jobs}; removed=[]; protected=[]
    hidden=set(view.get('hidden_rows',[]))
    for item in selected:
        if item.startswith('job:'):
            job=by_id.get(item[4:])
            if job is None: continue
            safe=not tracked_work(job)
        elif item.startswith('group:'):
            group=item[6:]; members=grouped.get(group)
            if not members: continue
            safe=not any(tracked_work(j) for j in members) and (joins or {}).get(group,{}).get('status')!='joining'
        else: continue
        if safe: hidden.add(item); removed.append(item)
        else: protected.append(item)
    view['hidden_rows']=sorted(hidden)
    return removed,protected

def positive_text_nodes(workflow):
    """Trace conditioning routes; exclude any text also reaching a negative input."""
    def trace(link, seen=None):
        seen=set() if seen is None else seen
        if not isinstance(link,list) or len(link)!=2: return set()
        key=str(link[0]); marker=(key,link[1])
        if marker in seen or key not in workflow: return set()
        seen.add(marker); node=workflow[key]; inputs=node['inputs']
        if node['class_type']=='CLIPTextEncode':
            return {key} if isinstance(inputs.get('text'),str) else set()
        if node['class_type']=='WanImageToVideo':
            return trace(inputs.get('positive' if link[1]==0 else 'negative'),seen) if link[1] in (0,1) else set()
        # Follow only named conditioning routes. Unknown custom routing requires an API export with a direct text route.
        result=set()
        for name,value in inputs.items():
            if name in ('conditioning','conditioning_1','conditioning_2','positive','negative','cond'):
                result |= trace(value,seen)
        return result
    positive=set(); negative=set()
    for node in workflow.values():
        if 'positive' in node['inputs']: positive |= trace(node['inputs']['positive'])
        if 'negative' in node['inputs']: negative |= trace(node['inputs']['negative'])
    return sorted(positive-negative)

def script_config(workflow, image_node, save_node, additions, selected='', main_prompt=None):
    candidates=positive_text_nodes(workflow)
    if not selected and len(candidates)==1: selected=candidates[0]
    if any(text.strip() for text in additions) and selected not in candidates:
        raise ValueError('Choose one traced positive text node. Shared negative nodes and unknown custom conditioning routes cannot be edited.')
    if main_prompt is not None and selected not in candidates: raise ValueError('Choose one safe positive text node before editing the main prompt')
    result={'workflow':copy.deepcopy(workflow),'image_node':image_node,'save_node':save_node,
            'positive_node':selected if selected in candidates else '', 'additions':list(additions)}
    if main_prompt is not None: result['main_prompt']=main_prompt
    return result

def editable_segment(job):
    return job.get('status')=='pending' and not any(job.get(key) for key in ('prompt_id','requested_prompt_id','submitted_prompt'))

def update_future_script(state, group, config, baseline, save, baseline_main=None):
    """Merge only edited, still-unsubmitted additions; recheck at save time."""
    with SCRIPT_LOCK:
        jobs=groups(state['jobs']).get(group,[])
        if not jobs: raise ValueError('Image group no longer exists')
        current=state.get('group_configs',{}).get(group)
        if current is None: raise ValueError('No saved script for this image group')
        for key in ('workflow','image_node','save_node','positive_node'):
            if config[key]!=current[key]: raise ValueError('Keep the saved workflow and positive node when editing queued segments')
        changed=[index for index,text in enumerate(config['additions']) if text!=baseline[index]]
        by_segment={job['segment']:job for job in jobs}
        for index in changed:
            if index not in by_segment or not editable_segment(by_segment[index]):
                raise ValueError(f'Segment {index+1} has started or was submitted while this editor was open. Reopen the script to edit later segments.')
            if current['additions'][index]!=baseline[index]: raise ValueError('Script changed while this editor was open; reopen it before saving')
        node=current['positive_node']
        current_main=current.get('main_prompt',current['workflow'].get(node,{}).get('inputs',{}).get('text',''))
        proposed=config.get('main_prompt',current_main)
        if proposed!=current_main:
            if baseline_main is not None and current_main!=baseline_main: raise ValueError('Main prompt changed while the editor was open; reopen it before saving')
            if node not in positive_text_nodes(current['workflow']): raise ValueError('Choose a safe positive text node for the main prompt')
            if not any(editable_segment(job) for job in jobs): raise ValueError('All segments have started; main prompt changes apply only to future segments')
        updated=copy.deepcopy(current)
        if 'main_prompt' in config: updated['main_prompt']=proposed
        for index in changed: updated['additions'][index]=config['additions'][index]
        state['group_configs'][group]=updated
        try: save()
        except Exception:
            state['group_configs'][group]=current; raise
        return len(changed)

RAW_FRAME_TITLE='WAN Batch Studio - lossless final frame'
RAW_FIRST_TITLE='WAN Batch Studio - lossless first frame'

def raw_first_node(prompt):
    matches=[key for key,node in prompt.items() if node.get('class_type')=='SaveImage' and node.get('_meta',{}).get('title')==RAW_FIRST_TITLE]
    if len(matches)>1: raise ValueError('Multiple raw first frame outputs')
    return matches[0] if matches else None

def raw_frame_node(prompt):
    matches=[key for key,node in prompt.items() if node.get('class_type')=='SaveImage' and node.get('_meta',{}).get('title')==RAW_FRAME_TITLE]
    if len(matches)>1: raise ValueError('Multiple raw final frame outputs in workflow')
    return matches[0] if matches else None

def add_raw_final_frame(prompt, save_node, job_id, include_first=False):
    """Branch the same IMAGE batch consumed by video encoding, preserving its graph."""
    if raw_frame_node(prompt): return prompt
    save=prompt[save_node]; source=None
    if save['class_type']=='SaveVideo':
        link=save['inputs'].get('video')
        if isinstance(link,list) and link[0] in prompt:
            create=prompt[link[0]]
            if create['class_type']=='CreateVideo': source=create['inputs'].get('images')
    elif save['class_type']=='VHS_VideoCombine': source=save['inputs'].get('images')
    if not isinstance(source,list):
        # Legacy/custom graphs cannot promise a pre-encoding image output.
        return prompt
    next_id=max([int(key) for key in prompt if str(key).isdigit()]+[0])+1
    select=str(next_id); output=str(next_id+1)
    prompt[select]={'class_type':'ImageFromBatch','inputs':{'image':copy.deepcopy(source),'batch_index':-1,'length':1},
                    '_meta':{'title':'WAN Batch Studio - select final decoded image'}}
    prompt[output]={'class_type':'SaveImage','inputs':{'images':[select,0],'filename_prefix':'WANBatchStudio/frames/'+job_id+'_last'},
                    '_meta':{'title':RAW_FRAME_TITLE}}
    if include_first:
        first_select=str(next_id+2); first_save=str(next_id+3)
        prompt[first_select]={'class_type':'ImageFromBatch','inputs':{'image':copy.deepcopy(source),'batch_index':0,'length':1}}
        prompt[first_save]={'class_type':'SaveImage','inputs':{'images':[first_select,0],'filename_prefix':'WANBatchStudio/frames/'+job_id+'_first'},'_meta':{'title':RAW_FIRST_TITLE}}
    return prompt

def segment_prompt(state, job, uploaded):
    config=state.get('group_configs',{}).get(job.get('group'))
    base=config['workflow'] if config else state['prompt']
    image_node=config['image_node'] if config else state['image_node']
    save_node=config['save_node'] if config else state['save_node']
    result=copy.deepcopy(base)
    if config:
        additions=config['additions']; addition=additions[job['segment']] if job['segment']<len(additions) else ''
        if addition.strip() or 'main_prompt' in config:
            node=config['positive_node']
            if node not in positive_text_nodes(base): raise ValueError('Saved positive node is not a safe positive text target')
            main=config.get('main_prompt',base[node]['inputs']['text'])
            result[node]['inputs']['text']=main+('\n'+addition if addition.strip() else '')
    result[image_node]['inputs']['image']=uploaded
    result[save_node]['inputs']['filename_prefix']='WANBatchStudio/'+job['id']
    return add_raw_final_frame(result,save_node,job['id'],include_first=state.get('auto_colour_match',True))

def colour_offsets(reference, first_frame, limit=16):
    """Robust per-channel boundary offset; preserve contrast and motion, cap correction."""
    import numpy as np
    from PIL import Image, ImageOps
    with Image.open(first_frame) as image: first=np.asarray(image.convert('RGB'),dtype=np.float32)
    with Image.open(reference) as image:
        target=np.asarray(ImageOps.fit(image.convert('RGB'),(first.shape[1],first.shape[0]),method=Image.Resampling.BILINEAR),dtype=np.float32)
    difference=target-first
    # Exclude large local differences caused by movement/reconstruction rather than colour.
    mask=np.max(np.abs(difference),axis=2)<48
    if mask.mean()<.25: raise PermanentFailure('First frame differs too much from input for safe automatic colour matching')
    measured=np.median(difference[mask],axis=0)
    offsets=np.clip(np.rint(measured),-limit,limit).astype(int)
    return {'method':'boundary_rgb_offset_v1','offset_rgb':offsets.tolist(),'measured_rgb':measured.tolist(),
            'limited':bool(np.any(np.abs(measured)>limit)),'matched_pixel_fraction':float(mask.mean())}

def apply_colour_match(reference, first_frame, original_video, original_frame, destination_video, destination_frame):
    import numpy as np
    from PIL import Image
    metadata=next(stream for stream in probe(original_video) if stream['codec_type']=='video')
    if metadata.get('color_transfer') in ('smpte2084','arib-std-b67') or metadata.get('color_space') not in (None,'unknown','bt709'):
        raise PermanentFailure('Automatic colour matching currently supports SDR BT.709 video only; originals retained')
    correction=colour_offsets(reference,first_frame); offsets=correction['offset_rgb']
    temporary=destination_video.with_name(destination_video.stem+'.part.mp4')
    frame_tmp=destination_frame.with_name(destination_frame.stem+'.part.png')
    source_range='pc' if metadata.get('color_range')=='pc' else 'tv'
    lut=':'.join(channel+"='clip(val+"+str(offset)+",0,255)'" for channel,offset in zip(('r','g','b'),offsets))
    filters=f'scale=in_color_matrix=bt709:in_range={source_range}:out_range=pc:flags=accurate_rnd+full_chroma_int,format=rgb24,lutrgb='+lut+',scale=out_color_matrix=bt709:in_range=pc:out_range=tv:flags=accurate_rnd+full_chroma_int,format=yuv420p'
    media_run(['ffmpeg','-v','error','-xerror','-y','-i',str(original_video),'-map','0:v:0','-map','0:a?',
               '-vf',filters,'-c:v','libx264','-crf','18','-preset','fast','-fps_mode','passthrough','-c:a','copy',
               '-color_range','tv','-colorspace','bt709','-color_primaries','bt709','-color_trc',metadata.get('color_transfer','iec61966-2-1'),
               '-movflags','+faststart',str(temporary)])
    checked=next(stream for stream in probe(temporary) if stream['codec_type']=='video')
    if checked.get('nb_read_frames')!=metadata.get('nb_read_frames') or (checked['width'],checked['height'])!=(metadata['width'],metadata['height']):
        raise PermanentFailure('Colour-matched video failed frame count/size verification; originals retained')
    media_run(['ffmpeg','-v','error','-xerror','-i',str(temporary),'-f','null','-'])
    with Image.open(original_frame) as image: values=np.asarray(image.convert('RGB'),dtype=np.int16)
    shifted=values+np.array(offsets,dtype=np.int16)
    correction['clipped_channel_fraction']=float(np.mean((shifted<0)|(shifted>255)))
    Image.fromarray(np.clip(shifted,0,255).astype(np.uint8)).save(frame_tmp,format='PNG')
    os.replace(temporary,destination_video); os.replace(frame_tmp,destination_frame)
    return correction

def export_script(path, config):
    # Complete base workflow is retained for reference; importing never silently replaces the current base.
    atomic(pathlib.Path(path),{'format':'WANBatchStudio.segment-script','version':1,'script':copy.deepcopy(config)})

def import_script(path):
    value=json.loads(pathlib.Path(path).read_text(encoding='utf-8-sig'))
    if value.get('format')!='WANBatchStudio.segment-script' or value.get('version')!=1: raise ValueError('Unsupported segment-script file')
    config=value.get('script',{}); additions=config.get('additions')
    if not isinstance(additions,list) or not 1<=len(additions)<=1001 or not all(isinstance(text,str) for text in additions): raise ValueError('Script must contain 1–1001 text additions')
    workflow=config.get('workflow')
    if not isinstance(workflow,dict): raise ValueError('Script has no reference workflow')
    return script_config(workflow,config['image_node'],config['save_node'],additions,config.get('positive_node',''),config.get('main_prompt'))

def cancel_owned(state, client, save, emit, timeout=12):
    """Called only by the state-owning worker after submission/download work has exited."""
    state['cancellation']={'status':'checking','requested_at':time.time()}; save()
    jobs=state['jobs']; unresolved=[]; settled=[]
    candidates=[j for j in jobs if j['status']!='done' and (j.get('prompt_id') or j.get('submitted_prompt') or j['status'] in ('submitting','running','cancel_unconfirmed'))]
    try:
        q=client.get('/queue') if candidates else {'queue_running':[],'queue_pending':[]}
        for j in candidates:
            if not j.get('prompt_id') and j.get('requested_prompt_id'):
                j['prompt_id']=j['requested_prompt_id']; save()
            if not j.get('prompt_id'):
                matches=[row for row in q['queue_running']+q['queue_pending'] if row[3].get('wan_batch_job')==j['id'] or row[3].get('client_id')==j['id']]
                if len(matches)==1: j['prompt_id']=matches[0][1]; save()
                else:
                    history=client.get('/history')
                    matches=[(pid,h) for pid,h in history.items() if isinstance(h.get('prompt'),list) and len(h['prompt'])>3 and (h['prompt'][3].get('wan_batch_job')==j['id'] or h['prompt'][3].get('client_id')==j['id'])]
                    if len(matches)==1: j['prompt_id']=matches[0][0]; j['cancel_history']=matches[0][1]; save()
                    else: unresolved.append(j); continue
            pid=j['prompt_id']; observed=False; deleted=False
            deadline=time.monotonic()+timeout
            while True:
                q=client.get('/queue')
                pending=[row[1] for row in q['queue_pending']]
                running=[row[1] for row in q['queue_running']]
                if pid in pending:
                    observed=True; j['cancel_observed']=True; save()
                    client.post('/queue',{'delete':[pid]}); deleted=True
                if pid in running:
                    observed=True; j['cancel_observed']=True; save()
                    if not client.targeted_interrupt_supported():
                        emit('Cancellation blocked: prompt-specific interruption is not verified for this server. No global interrupt sent.')
                        unresolved.append(j); break
                    # Refresh identity directly before the targeted request. Never use an empty/global payload.
                    latest=client.get('/queue')
                    if pid in [row[1] for row in latest['queue_running']]:
                        client.post('/interrupt',{'prompt_id':pid})
                latest=client.get('/queue')
                if pid not in [row[1] for row in latest['queue_running']+latest['queue_pending']]:
                    history=client.get('/history/'+pid).get(pid)
                    if history: j['cancel_history']=history; record_render_history(j,history)
                    if history or observed or deleted or j.get('cancel_history') or j.get('cancel_observed'):
                        timing=j.setdefault('render_timing',{})
                        if timing.get('phase') not in ('complete','failed','cancelled'):
                            timing['phase']='cancelled'
                            if timing.get('start_wall') is not None: timing.update(seconds=max(0,time.time()-timing['start_wall']),duration_source='observed cancellation')
                        settled.append(j); break
                    unresolved.append(j); break
                if time.monotonic()>=deadline: unresolved.append(j); break
                time.sleep(.2)
    except Exception as e:
        emit('Cancellation could not be confirmed: '+str(e))
        unresolved=[j for j in candidates if j not in settled]
    archive={'time':time.time(),'jobs':copy.deepcopy(jobs),'group_configs':copy.deepcopy(state.get('group_configs',{})),'joins':copy.deepcopy(state.get('joins',{}))}
    state.setdefault('cancel_archives',[]).append(archive)
    for j in unresolved:
        j.setdefault('status_before_cancel',j['status']); j['status']='cancel_unconfirmed'
    state['jobs']=unresolved
    keep={j.get('group') for j in unresolved}
    state['group_configs']={k:v for k,v in state.get('group_configs',{}).items() if k in keep}
    state['joins']={k:v for k,v in state.get('joins',{}).items() if k in keep}
    state['cancellation']['status']='unconfirmed' if unresolved else 'cleared'
    save()
    emit('Cancellation unconfirmed: recovery tracking retained. Click Cancel again to reconcile; Start is blocked.' if unresolved else 'Cancelled and cleared the app queue. Original images and completed outputs are preserved.')
    return not unresolved

def prepare_reset_retry(state, client, save):
    if not state.get('reset_retry_requested'): return
    snapshot=client.get('/queue')
    if snapshot.get('queue_running') != [] or snapshot.get('queue_pending') != []:
        raise ValueError('Wait for ComfyUI to become idle before restarting reset work. Existing jobs are untouched.')
    # Read history before changing anything. A successful prompt must be downloaded, not rerendered.
    history=client.get('/history')
    if not isinstance(history,dict): raise ValueError('Cannot verify ComfyUI history')
    previous=copy.deepcopy(state)
    working=copy.deepcopy(state)
    for job in working['jobs']:
        if job['status']=='done': continue
        pid=job.get('prompt_id') or job.get('requested_prompt_id')
        matches=[key for key,h in history.items() if key==pid or
                 (isinstance(h.get('prompt'),list) and len(h['prompt'])>3 and
                  (h['prompt'][3].get('wan_batch_job')==job['id'] or h['prompt'][3].get('client_id')==job['id']))]
        if len(matches)>1: raise ValueError('Multiple historical prompts match this job; recovery tracking retained')
        if matches and history[matches[0]].get('status',{}).get('status_str')!='error':
            job.update(prompt_id=matches[0],status='running'); continue
        job.setdefault('reset_attempts',[]).append({key:copy.deepcopy(job[key]) for key in
            ('prompt_id','requested_prompt_id','submitted_prompt','render_timing','status','execution_failure') if key in job})
        for key in ('prompt_id','requested_prompt_id','submitted_prompt','render_timing','execution_failure'):
            job.pop(key,None)
        job['status']='pending'
    working.pop('reset_retry_requested',None)
    state.clear(); state.update(working)
    try: save()
    except Exception:
        state.clear(); state.update(previous); raise

def clear_idle_queue(state, client, save):
    """Archive local work only after a successful, idle backend observation. No POSTs."""
    snapshot=client.get('/queue')
    if snapshot.get('queue_running') != [] or snapshot.get('queue_pending') != []:
        raise ValueError('ComfyUI is busy or its queue state is unknown. Pause and wait, or use Cancel render and clear queue for app-owned work.')
    previous=copy.deepcopy(state)
    count=len(state.get('jobs',[])); now=time.time()
    state.setdefault('clear_archives',[]).append({'time':now,'reason':'user_clear_idle_queue',
        'jobs':copy.deepcopy(state.get('jobs',[])), 'group_configs':copy.deepcopy(state.get('group_configs',{})),
        'joins':copy.deepcopy(state.get('joins',{})), 'cancellation':copy.deepcopy(state.get('cancellation',{})),
        'worker_state':copy.deepcopy(state.get('worker_state',{})), 'recovery':copy.deepcopy(state.get('recovery',{}))})
    state.update(jobs=[],group_configs={},joins={},cancellation={'status':'cleared','time':now},
                 worker_state={'status':'idle','time':now},recovery={'status':'idle','phase':'idle','time':now})
    try: save()
    except Exception:
        state.clear(); state.update(previous); raise
    return count

class App:
    def __init__(self, root):
        self.root=root; root.title('WAN Batch Studio'); root.geometry(f'{min(1280,root.winfo_screenwidth()-40)}x{min(780,root.winfo_screenheight()-100)}'); root.minsize(640,440)
        root.rowconfigure(1,weight=1); root.columnconfigure(0,weight=1)
        self.events=queue.Queue(); self.worker_thread=None; self.running=False; self.halt=threading.Event(); self.cancel=threading.Event()
        self.after_ids=set()
        self.thumbnail_loader=ThumbnailLoader(); self.thumb_cache=OrderedDict(); self.thumb_pending=set(); self.row_images={}; self.row_preview_paths={}
        self.state=load_queue_state(STATE)
        self.state_path=STATE; self.diagnostic_root=STATE.parent; self.phase='idle'; self.current_job=None
        try:
            persisted=json.loads(STATE.with_name('recovery_status.json').read_text())
            if isinstance(persisted,dict) and isinstance(persisted.get('time',0),(int,float)) and persisted.get('time',0)>self.state.get('recovery',{}).get('time',0): self.state['recovery']=persisted
        except (OSError,ValueError): pass
        self.view_path=STATE.with_name('view_state.json')
        try: self.view=json.loads(self.view_path.read_text()) if self.view_path.exists() else {'hidden_rows':[]}
        except (OSError,ValueError): self.view={'hidden_rows':[]}
        for job in self.state['jobs']:
            if job.get('history') and (not job.get('render_timing') or 'server_end' not in job['render_timing']): record_render_history(job,job['history'])
        self.url=tk.StringVar(value=self.state.get('url','http://127.0.0.1:8000'))
        self.workflow=tk.StringVar(value=self.state.get('workflow',''))
        self.output=tk.StringVar(value=self.state.get('output',str(ROOT/'outputs')))
        self.count=tk.IntVar(value=self.state.get('total_segments',1))
        self.autojoin=tk.BooleanVar(value=self.state.get('auto_join',False))
        self.autocolour=tk.BooleanVar(value=self.state.get('auto_colour_match',True))
        self.settings_outer=ttk.LabelFrame(root,text='Workflow, images and queue options - scroll for more')
        self.settings_outer.grid(row=0,column=0,sticky='ew',padx=8,pady=4)
        self.settings_outer.columnconfigure(0,weight=1); self.settings_outer.rowconfigure(0,weight=1)
        self.settings_canvas=tk.Canvas(self.settings_outer,height=220,highlightthickness=0)
        self.settings_canvas.grid(row=0,column=0,sticky='nsew')
        settings_vertical=ttk.Scrollbar(self.settings_outer,orient='vertical',command=self.settings_canvas.yview); settings_vertical.grid(row=0,column=1,sticky='ns')
        settings_horizontal=ttk.Scrollbar(self.settings_outer,orient='horizontal',command=self.settings_canvas.xview); settings_horizontal.grid(row=1,column=0,sticky='ew')
        self.settings_canvas.configure(yscrollcommand=settings_vertical.set,xscrollcommand=settings_horizontal.set)
        settings=ttk.Frame(self.settings_canvas); self.settings_content=settings
        self.settings_window=self.settings_canvas.create_window((0,0),window=settings,anchor='nw')
        settings.bind('<Configure>',self.resize_settings)
        self.settings_canvas.bind('<Configure>',self.resize_settings)
        self.settings_canvas.bind('<MouseWheel>',lambda event:self.settings_canvas.yview_scroll(-int(event.delta/120),'units'))
        for label,var,command in [('ComfyUI',self.url,None),('API workflow',self.workflow,self.pick_workflow),('Output folder',self.output,self.pick_output)]:
            row=ttk.Frame(settings); row.pack(fill='x',padx=12,pady=5); ttk.Label(row,text=label,width=16).pack(side='left'); ttk.Entry(row,textvariable=var).pack(side='left',fill='x',expand=True)
            if command: ttk.Button(row,text='Browse',command=command).pack(side='left')
        row=ttk.Frame(settings); row.pack(fill='x',padx=12,pady=8)
        ttk.Button(row,text='Add images',command=self.add_images).pack(side='left'); ttk.Button(row,text='Add folder',command=self.add_folder).pack(side='left')
        ttk.Label(row,text=' Total segments per image:').pack(side='left'); ttk.Spinbox(row,from_=1,to=1001,textvariable=self.count,width=5).pack(side='left')
        self.total=tk.StringVar(); self.count.trace_add('write',self.update_total); self.update_total()
        ttk.Label(settings,textvariable=self.total).pack(anchor='w',padx=12)
        ttk.Checkbutton(settings,text='Automatically match segment colour to its starting input (keep originals)',variable=self.autocolour).pack(anchor='w',padx=12)
        ttk.Checkbutton(settings,text='Automatically join each image’s segments (remove first frame of each continuation)',variable=self.autojoin).pack(anchor='w',padx=12)
        scriptrow=ttk.Frame(settings); scriptrow.pack(fill='x',padx=12,pady=4)
        ttk.Button(scriptrow,text='Segment prompts for new images…',command=self.edit_template).pack(side='left')
        ttk.Button(scriptrow,text='Edit selected image script…',command=self.edit_selected_script).pack(side='left')
        ttk.Label(scriptrow,text='Additions are saved per image group; blank keeps the base prompt.').pack(side='left',padx=6)
        removal=ttk.Frame(settings); removal.pack(fill='x',padx=12,pady=5)
        ttk.Button(removal,text='Remove selected rows from list',command=self.remove_selected).pack(side='left')
        ttk.Button(removal,text='Show hidden rows',command=self.show_hidden).pack(side='left')
        ttk.Button(removal,text='Cancel / Reset stuck job',command=self.reset_stuck).pack(side='left')
        ttk.Button(removal,text='Clear Queue',command=self.clear_queue).pack(side='left')
        ttk.Button(removal,text='Drop unstarted groups from queue',command=self.clear_pending).pack(side='left')
        self.queue_feedback=tk.StringVar(value='List cleanup hides only selected rows. Files, scheduled jobs, chaining and joins stay unchanged. Right-click a row for actions.')
        ttk.Label(settings,textvariable=self.queue_feedback,wraplength=980).pack(fill='x',padx=12,pady=3)
        table=ttk.Frame(root); table.grid(row=1,column=0,sticky='nsew',padx=8,pady=4)
        table.rowconfigure(0,weight=1); table.columnconfigure(0,weight=1)
        self.tree=ttk.Treeview(table,columns=('input','status','render_start','render_end'),show='tree headings',selectmode='extended')
        self.tree.bind('<Button-3>',self.context_menu)
        style=ttk.Style(root); style.configure('WAN.Treeview',rowheight=64); self.tree.configure(style='WAN.Treeview')
        self.placeholder=tk.PhotoImage(master=root,width=64,height=56); self.placeholder.put('#d5d9df',to=(0,0,64,56))
        self.tree.heading('#0',text='Preview / source image / segment'); self.tree.column('#0',width=310,minwidth=200)
        for c in ('input','status'): self.tree.heading(c,text=c.title()); self.tree.column(c,width=210,minwidth=150)
        for c,title in (('render_start','Render start (London)'),('render_end','Render end (London)')):
            self.tree.heading(c,text=title); self.tree.column(c,width=235,minwidth=210,stretch=False)
        self.tree.grid(row=0,column=0,sticky='nsew')
        vertical=ttk.Scrollbar(table,orient='vertical',command=self.tree.yview); vertical.grid(row=0,column=1,sticky='ns')
        horizontal=ttk.Scrollbar(table,orient='horizontal',command=self.tree.xview); horizontal.grid(row=1,column=0,sticky='ew')
        self.tree.configure(yscrollcommand=vertical.set,xscrollcommand=horizontal.set)
        self.footer=ttk.Frame(root); self.footer.grid(row=2,column=0,sticky='ew',padx=8,pady=(0,6)); self.footer.columnconfigure(0,weight=1)
        self.run_controls=ttk.Frame(self.footer); self.run_controls.grid(row=0,column=0,sticky='ew')
        self.run_buttons={}
        for label,cmd in [('Start / Resume',self.start),('Pause after current',self.pause),('Stop after current',self.pause),('Inspect workflow',self.inspect),('Join / Retry joins only',self.retry_joins),('Cancel render and clear queue',self.cancel_all)]:
            self.run_buttons[label]=ttk.Button(self.run_controls,text=label,command=cmd)
        self.control_columns=None
        self.status=tk.StringVar(value='Idle. Import the exact workflow in API format; no model substitutions.')
        if self.state.get('cancellation',{}).get('status') in ('checking','unconfirmed'):
            self.status.set('Cancellation recovery retained. Click Cancel render and clear queue to reconcile; Start remains blocked.')
        if self.state.get('startup_notice'): self.status.set(self.state['startup_notice'])
        self.status_label=ttk.Label(self.footer,textvariable=self.status,wraplength=880)
        self.status_label.grid(row=1,column=0,sticky='ew',padx=4,pady=4)
        self.timer_text=tk.StringVar(value='Render timer: idle')
        self.timer_label=ttk.Label(self.footer,textvariable=self.timer_text,font=('Segoe UI',12,'bold'),wraplength=880)
        self.timer_label.grid(row=2,column=0,sticky='ew',padx=4,pady=4)
        self.recovery_text=tk.StringVar(value=recovery_summary(self.state))
        recovery_bar=ttk.Frame(self.footer); recovery_bar.grid(row=3,column=0,sticky='ew',padx=4,pady=2); recovery_bar.columnconfigure(0,weight=1)
        self.recovery_label=ttk.Label(recovery_bar,textvariable=self.recovery_text)
        self.recovery_label.grid(row=0,column=0,sticky='ew')
        ttk.Button(recovery_bar,text='Recovery details',command=self.show_recovery_details).grid(row=0,column=1,padx=(6,0))
        root.bind('<Configure>',self.resize_layout,add='+'); self.footer.bind('<Configure>',self.resize_layout)
        self.resize_layout()
        root.protocol('WM_DELETE_WINDOW',self.close); root.bind('<Destroy>',self.thumbnail_destroy,add='+'); self.refresh(); self.poll(); self.timer_tick(); self.thumbnail_tick()
    def resize_settings(self,event=None):
        width=max(self.settings_canvas.winfo_width(),self.settings_content.winfo_reqwidth())
        self.settings_canvas.itemconfigure(self.settings_window,width=width)
        self.settings_canvas.configure(scrollregion=self.settings_canvas.bbox('all'))
    def resize_layout(self,event=None):
        if event is not None and event.widget not in (self.root,self.footer): return
        width=max(1,self.root.winfo_width()-24)
        longest=max(button.winfo_reqwidth() for button in self.run_buttons.values())
        columns=max(1,min(3,width//(longest+12)))
        if columns!=self.control_columns:
            self.control_columns=columns
            for index in range(3): self.run_controls.columnconfigure(index,weight=1 if index<columns else 0)
            for index,button in enumerate(self.run_buttons.values()): button.grid(row=index//columns,column=index%columns,sticky='ew',padx=4,pady=3)
        self.status_label.configure(wraplength=max(200,width-8)); self.timer_label.configure(wraplength=max(200,width-8))
        available=max(44,min(int(self.root.winfo_height()*.34),self.root.winfo_height()-self.footer.winfo_reqheight()-120))
        self.settings_canvas.configure(height=available)
    def pick_workflow(self):
        if self.running: return
        p=filedialog.askopenfilename(filetypes=[('Workflow JSON','*.json')])
        if p: self.workflow.set(p); self.inspect()
    def pick_output(self):
        p=filedialog.askdirectory()
        if p: self.output.set(p)
    def inspect(self):
        try: w,_,_=load_prompt(self.workflow.get()); messagebox.showinfo('Workflow settings',describe(w))
        except Exception as e: messagebox.showerror('Workflow',str(e))
    def add_images(self): self.add(filedialog.askopenfilenames(filetypes=[('Images','*.png *.jpg *.jpeg *.webp *.bmp')]))
    def add_folder(self):
        p=filedialog.askdirectory()
        if p: self.add(sorted(str(x) for x in pathlib.Path(p).iterdir() if x.suffix.lower() in ('.png','.jpg','.jpeg','.webp','.bmp')))
    def add(self, paths):
        if self.running: return
        if self.state.get('cancellation',{}).get('status') in ('checking','unconfirmed'):
            self.status.set('Resolve cancellation with Cancel render and clear queue before adding work.'); return
        if not paths: return
        try:
            count=self.count.get()
            if not 1<=count<=1001: raise ValueError()
        except (ValueError,tk.TclError): messagebox.showerror('Segments','Enter total segments from 1 to 1001.'); return
        try:
            w,i,s=load_prompt(self.workflow.get()); template=self.state.get('script_template',{})
            # A template belongs to the workflow it was edited against; never reuse target IDs in another graph.
            if template and template.get('workflow')!=w:
                raise ValueError('Workflow changed. Open Segment prompts for new images and save the script for this workflow, or clear its additions there.')
            additions=(template.get('additions',[])+['']*count)[:count]
            config=script_config(w,i,s,additions,template.get('positive_node',''),template.get('main_prompt'))
        except Exception as e: messagebox.showerror('Cannot add images',str(e)); return
        for p in paths:
            group=uuid.uuid4().hex
            self.state.setdefault('group_configs',{})[group]=copy.deepcopy(config)
            for seg in range(count): self.state['jobs'].append({'id':uuid.uuid4().hex,'group':group,'source':str(p),'segment':seg,'status':'pending'})
        self.state['total_segments']=count
        self.save(); self.refresh()
    def edit_template(self): self.edit_script()
    def edit_selected_script(self):
        selected={(self.tree.parent(item) or item).removeprefix('group:') for item in self.tree.selection()}
        if len(selected)!=1: messagebox.showinfo('Select image','Select exactly one image group or its segment.'); return
        self.edit_script(next(iter(selected)))
    def edit_script(self,group=None):
        if self.running and not group: messagebox.showinfo('Worker active','Pause after current before editing new-image scripts.'); return
        try:
            jobs=groups(self.state['jobs']).get(group,[]) if group else []
            if group and not jobs: raise ValueError('Image group no longer exists')
            readonly=bool(group and not any(editable_segment(j) for j in jobs))
            saved=copy.deepcopy(self.state.get('group_configs',{}).get(group,{}) if group else self.state.get('script_template',{}))
            if group and saved: w=saved['workflow']; i=saved['image_node']; s=saved['save_node']
            else: w,i,s=load_prompt(self.workflow.get())
            count=len(jobs) if group else self.count.get()
            if not 1<=count<=1001: raise ValueError('Enter total segments from 1 to 1001.')
            candidates=positive_text_nodes(w)
        except Exception as e: messagebox.showerror('Script',str(e)); return
        dialog=tk.Toplevel(self.root); dialog.title('Image script (read only; export available)' if readonly else ('Image script' if group else 'Script for newly added images')); dialog.geometry('800x650'); dialog.transient(self.root)
        explanation=tk.StringVar(value=f'{count} total segments. Each addition is appended once to the main positive prompt. Main prompt changes apply to remaining unsubmitted segments. Submitted/completed segments are locked; future segments remain editable.')
        ttk.Label(dialog,textvariable=explanation,wraplength=770).pack(padx=10,pady=8)
        target=tk.StringVar(value=saved.get('positive_node','') if saved.get('positive_node') in candidates else (candidates[0] if len(candidates)==1 else ''))
        ttk.Label(dialog,text='Positive text node ID (choose explicitly if multiple; negative/shared nodes excluded):').pack(anchor='w',padx=10)
        selector=ttk.Combobox(dialog,textvariable=target,values=candidates,state='readonly'); selector.pack(fill='x',padx=10)
        if readonly or group: selector.configure(state='disabled')
        ttk.Label(dialog,text='Main positive prompt (applies to future segments):').pack(anchor='w',padx=10)
        preview=tk.Text(dialog,height=5,wrap='word'); preview.pack(fill='x',padx=10,pady=5)
        def show_base(*args):
            preview.configure(state='normal'); preview.delete('1.0','end')
            preview.insert('1.0',saved.get('main_prompt',w.get(target.get(),{}).get('inputs',{}).get('text','')) if target.get()==saved.get('positive_node',target.get()) else w.get(target.get(),{}).get('inputs',{}).get('text',''))
            preview.configure(state='disabled' if readonly or target.get() not in candidates else 'normal')
        target.trace_add('write',show_base); show_base()
        holder=ttk.Frame(dialog); holder.pack(fill='both',expand=True,padx=10)
        canvas=tk.Canvas(holder,highlightthickness=0); scrollbar=ttk.Scrollbar(holder,orient='vertical',command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set); scrollbar.pack(side='right',fill='y'); canvas.pack(side='left',fill='both',expand=True)
        fields=ttk.Frame(canvas); window=canvas.create_window((0,0),window=fields,anchor='nw')
        fields.bind('<Configure>',lambda e:canvas.configure(scrollregion=canvas.bbox('all')))
        canvas.bind('<Configure>',lambda e:canvas.itemconfigure(window,width=e.width))
        entries=[]
        def populate(additions):
            entries.clear()
            for child in fields.winfo_children(): child.destroy()
            for seg in range(count):
                ttk.Label(fields,text=f'Segment {seg+1} — '+('original image' if seg==0 else f'last frame from segment {seg}')).pack(anchor='w',pady=(6,0))
                entry=tk.Text(fields,height=2,wrap='word'); entry.pack(fill='x')
                entry.insert('1.0',additions[seg] if seg<len(additions) else '')
                if readonly or (group and not editable_segment(jobs[seg])): entry.configure(state='disabled')
                entries.append(entry)
        populate(saved.get('additions',[]))
        def current_config(): return script_config(w,i,s,[entry.get('1.0','end-1c') for entry in entries],target.get(),preview.get('1.0','end-1c') if target.get() in candidates else None)
        def export_current():
            try:
                config=current_config()
                path=filedialog.asksaveasfilename(parent=dialog,title='Keep segment script',defaultextension='.json',initialfile='segment_script.json',filetypes=[('WAN segment script','*.json')])
                if path: export_script(path,config)
            except Exception as e: messagebox.showerror('Export script',str(e),parent=dialog)
        def import_actions():
            nonlocal count
            path=filedialog.askopenfilename(parent=dialog,title='Reuse segment script actions',filetypes=[('WAN segment script','*.json')])
            if not path: return
            try:
                imported=import_script(path); additions=imported['additions']
                if group and len(additions)!=count: raise ValueError(f'This group has {count} segments; imported script has {len(additions)}. Import into a new-image template to use its segment count.')
                count=len(additions)
                if not group: self.count.set(count)
                target.set(imported['positive_node'] if imported['workflow']==w else (candidates[0] if len(candidates)==1 else ''))
                populate(additions)
                explanation.set(f'{count} segments imported. Actions only; CURRENT workflow/base and negative prompts are preserved. Save to apply.')
            except Exception as e: messagebox.showerror('Import script',str(e),parent=dialog)
        transfer=ttk.Frame(dialog); transfer.pack(fill='x',padx=10,pady=4)
        ttk.Button(transfer,text='Export script…',command=export_current).pack(side='left')
        if not readonly and not group: ttk.Button(transfer,text='Import / reuse actions…',command=import_actions).pack(side='left')
        ttk.Label(transfer,text='Import keeps the current base prompt.').pack(side='left',padx=8)
        def commit():
            try: config=current_config()
            except Exception as e: messagebox.showerror('Script',str(e),parent=dialog); return
            try:
                if group: update_future_script(self.state,group,config,saved.get('additions',['']*count),self.save,baseline_main=saved.get('main_prompt',w.get(saved.get('positive_node',target.get()),{}).get('inputs',{}).get('text','')))
                else: self.state['script_template']=config; self.save()
            except Exception as e: messagebox.showerror('Script',str(e),parent=dialog); return
            self.refresh(); dialog.destroy()
        if not readonly: ttk.Button(dialog,text='Save image script' if group else 'Save for newly added images',command=commit).pack(pady=8)
        dialog.grab_set()
    def update_total(self,*args):
        try: self.total.set(f'Add each image once: {self.count.get()} total = 1 original + {max(0,self.count.get()-1)} continuations. Each continuation uses the previous segment’s last decoded frame.')
        except tk.TclError: self.total.set('Enter a whole number of total segments.')
    def remove_selected(self):
        selected={item for item in self.tree.selection() if self.tree.exists(item)}
        if not selected:
            self.removal_feedback('Nothing selected. Select segment rows or image-group rows to remove from the visible list.',blocked=True); return
        self.hide_selected_rows(selected)
    def hide_selected_rows(self, selected):
        before=copy.deepcopy(self.view)
        removed,protected=hide_list_rows(self.view,self.state['jobs'],selected,self.state.get('joins',{}))
        if removed:
            try: atomic(self.view_path,self.view)
            except Exception as e:
                self.view=before; self.removal_feedback('List cleanup was not applied because view preferences could not be saved: '+str(e),blocked=True); return
            self.refresh()
        text=f'Removed {len(removed)} selected row(s) from the list only. All files and scheduled work are unchanged; use Show hidden rows to restore.'
        if protected: text+=f' Kept {len(protected)} active/uncertain row(s) visible; their tracking is protected.'
        if not removed and not protected: text='Those rows are no longer present. Select current rows again.'
        self.removal_feedback(text,blocked=bool(protected and not removed))
    def show_hidden(self):
        before=copy.deepcopy(self.view); self.view['hidden_rows']=[]
        try: atomic(self.view_path,self.view)
        except Exception as e:
            self.view=before; self.removal_feedback('Could not restore hidden rows: '+str(e),blocked=True); return
        self.refresh(); self.removal_feedback('All hidden rows are visible again. No job or file was changed.')
    def context_menu(self,event):
        item=self.tree.identify_row(event.y)
        if not item: return 'break'
        if item not in self.tree.selection(): self.tree.selection_set(item)
        self.tree.focus(item)
        group=(self.tree.parent(item) or item).removeprefix('group:')
        selected_segments=[row for row in self.tree.selection() if row.startswith('job:')]
        selected_groups=[row for row in self.tree.selection() if row.startswith('group:')]
        menu=tk.Menu(self.root,tearoff=False); self.row_menu=menu
        menu.add_command(label='Remove selected segment rows from list',state='normal' if selected_segments else 'disabled',command=lambda:self.hide_selected_rows(selected_segments))
        menu.add_command(label='Remove selected image-group rows from list',state='normal' if selected_groups else 'disabled',command=lambda:self.hide_selected_rows(selected_groups))
        menu.add_command(label='Remove this whole image group from list',command=lambda:self.hide_selected_rows({'group:'+group}))
        menu.add_command(label='Show all hidden rows',command=self.show_hidden)
        menu.add_separator()
        menu.add_command(label='View / edit image script…',command=lambda:self.edit_script(group))
        job=next((j for j in self.state['jobs'] if 'job:'+j['id']==item),None)
        output=job.get('video') if job else self.state.get('joins',{}).get(group,{}).get('video')
        folder=pathlib.Path(output).parent if output else None
        menu.add_command(label='Open output folder',state='normal' if folder and folder.is_dir() else 'disabled',command=lambda:os.startfile(str(folder)))
        try: menu.tk_popup(event.x_root,event.y_root)
        finally: menu.grab_release()
        return 'break'
    def clear_pending(self): self.remove_groups(None)
    def removal_feedback(self,text,blocked=False):
        self.queue_feedback.set(text); self.status.set(text)
        if blocked: messagebox.showinfo('Queue removal',text,parent=self.root)
    def remove_groups(self,selected):
        if selected is not None:
            self.hide_selected_rows({'group:'+key for key in selected}); return
        if self.running:
            self.removal_feedback('Queue is active. Click Pause after current, wait for the segment/local processing to finish, then Remove selected. To stop an app-owned render now, use Cancel render and clear queue.',blocked=True); return
        existing=set(groups(self.state['jobs']))
        if selected is not None and not (selected & existing):
            self.removal_feedback('Selected rows are no longer in the queue. Select an image group again.',blocked=True); return
        before=copy.deepcopy(self.state)
        removed,protected=remove_pending_groups(self.state)
        if removed:
            try: self.save()
            except Exception as e:
                self.state=before; self.refresh(); self.removal_feedback('Removal was not applied because the queue could not be saved: '+str(e),blocked=True); return
            self.refresh()
        if protected:
            reason='Dropping from the queue only affects wholly unstarted groups. Use list cleanup to hide completed or partly completed rows without changing scheduled work.' if selected is None else 'Protected groups have active/uncertain submitted work or an unfinished join. Use Cancel render and clear queue to reconcile app-owned work; their tracking has been retained.'
            text=f'Removed {len(removed)} image group(s); kept {len(protected)}. '+reason+' All source/output files are preserved.'
            self.removal_feedback(text,blocked=not removed)
        else:
            self.removal_feedback(f'Removed {len(removed)} whole image group(s) from the queue. Completed records are archived; all source/output files are preserved.' if selected is not None else f'Cleared {len(removed)} unstarted image group(s). All source/output files are preserved.')
    def join_ready(self):
        for group,jobs in groups(self.state['jobs']).items():
            check_cancel()
            if not all(j['status']=='done' for j in jobs): continue
            record=self.state.setdefault('joins',{}).setdefault(group,{})
            if record.get('status')=='done' and pathlib.Path(record['video']).exists(): continue
            try:
                if [j['segment'] for j in jobs]!=list(range(len(jobs))): raise ValueError('Segment order is not contiguous')
                joined_dir=pathlib.Path(jobs[0]['video']).parent/'joined'
                joined_dir.mkdir(parents=True,exist_ok=True)
                target=joined_dir/(group+'_joined.mp4')
                record.update(status='joining'); self.save(); self.emit('Joining '+pathlib.Path(jobs[0]['source']).name)
                frames=self.recover('join_segments',lambda:join_segments([j['video'] for j in jobs],target))
                record.update(status='done',video=str(target),frames=frames,error=None); self.emit('Joined: '+str(target))
            except Cancelled: raise
            except Exception as e:
                record.update(status='error',error=error_summary(e)); self.diagnostic('join_failed',e)
                self.state['recovery']={'status':'attention','phase':'join_segments','reason':error_summary(e),'time':time.time()}; self.persist_recovery()
                self.emit('Join failed; segments retained. Retry joins only: '+error_summary(e))
            self.save()
    def retry_joins(self):
        if self.running: return
        if self.state.get('cancellation',{}).get('status') in ('checking','unconfirmed'): return
        self.cancel.clear()
        self.running=True
        def run():
            MEDIA_CONTEXT.cancel=self.cancel
            try: self.join_ready()
            except Cancelled: pass
            finally: self.finish_worker()
        self.launch_worker(run)
    def diagnostic(self,event,error=None,**fields):
        job=getattr(self,'current_job',None) or {}
        record={'time_utc':datetime.now(ZoneInfo('UTC')).isoformat(),'time_london':datetime.now(LONDON).isoformat(),
                'event':event,'phase':getattr(self,'phase','unknown'),'job_id':job.get('id'),
                'segment':job.get('segment'),'prompt_id':job.get('prompt_id') or job.get('requested_prompt_id'),**fields}
        if error is not None:
            record.update(exception=type(error).__name__,reason=error_summary(error),
                cause_type=type(getattr(error,'reason',None) or error.__cause__).__name__,
                trace=[{'file':pathlib.Path(frame.filename).name,'line':frame.lineno,'function':frame.name} for frame in traceback.extract_tb(error.__traceback__)])
            if isinstance(error,subprocess.CalledProcessError):
                stderr=error.stderr.decode('utf-8',errors='replace') if isinstance(error.stderr,bytes) else str(error.stderr or '')
                record['process_error_tags']=[marker for marker in ('permission denied','no space left','invalid data','no such file','unknown encoder','sharing violation','input/output error') if marker in stderr.lower()]
        try:
            folder=pathlib.Path(getattr(self,'diagnostic_root',pathlib.Path(self.state.get('output',tempfile.gettempdir()))/'diagnostics'))/'logs'
            folder.mkdir(parents=True,exist_ok=True); path=folder/'recovery.jsonl'
            if path.exists() and path.stat().st_size>2*1024*1024:
                for index in range(3,0,-1):
                    old=path.with_name('recovery.jsonl.'+str(index)); newer=path.with_name('recovery.jsonl.'+str(index+1))
                    if old.exists(): os.replace(old,newer)
                os.replace(path,path.with_name('recovery.jsonl.1'))
            with open(path,'a',encoding='utf-8') as stream:
                stream.write(json.dumps(record,ensure_ascii=True)+'\n'); stream.flush()
                if error is not None: os.fsync(stream.fileno())
            self.state.pop('logging_unavailable',None)
        except Exception:
            self.state['logging_unavailable']=True
    def persist_recovery(self):
        # Separate sidecar still records a queue-file sharing violation without recursion.
        path=getattr(self,'state_path',None)
        if path is None: return
        try:
            atomic(path.with_name('recovery_status.json'),self.state.get('recovery',{})); self.state.pop('recovery_persistence_unavailable',None)
        except Exception: self.state['recovery_persistence_unavailable']=True
    def recover(self,phase,operation,cancelable=True,delays=None):
        self.phase=phase; delays=getattr(self,'retry_delays',delays if delays is not None else RETRY_DELAYS)
        for attempt in range(len(delays)+1):
            if cancelable: check_cancel()
            try:
                value=operation()
                if attempt:
                    self.state['recovery']={'status':'recovered','phase':phase,'attempt':attempt+1,'time':time.time()}
                    self.persist_recovery(); self.diagnostic('operation_recovered',attempt=attempt+1); self.events.put(None)
                return value
            except Cancelled: raise
            except Exception as error:
                retry=temporary_error(error) and attempt<len(delays)
                self.diagnostic('retry_scheduled' if retry else 'operation_failed',error,attempt=attempt+1,limit=len(delays)+1)
                if not retry:
                    self.state['recovery']={'status':'stopped','phase':phase,'reason':error_summary(error),'job_id':(getattr(self,'current_job',None) or {}).get('id'),'time':time.time()}
                    self.persist_recovery(); self.events.put(None); raise
                delay=delays[attempt]
                self.state['recovery']={'status':'retrying','phase':phase,'attempt':attempt+1,'limit':len(delays)+1,'delay':delay,'reason':error_summary(error),'time':time.time()}
                self.persist_recovery(); self.events.put(None)
                if cancelable:
                    if getattr(self,'cancel',threading.Event()).wait(delay): raise Cancelled('Cancellation requested during recovery')
                else: time.sleep(delay)
    def save(self):
        with SCRIPT_LOCK:
            self.recover('save_queue',lambda:atomic(getattr(self,'state_path',STATE),self.state),cancelable=False,delays=(.2,.5,1,2))
    def stop_reason(self,error):
        self.state['recovery']={'status':'stopped','phase':getattr(self,'phase','unknown'),'reason':error_summary(error),'job_id':(getattr(self,'current_job',None) or {}).get('id'),'time':time.time()}
        self.persist_recovery(); self.diagnostic('worker_stopped',error)
        try:
            if hasattr(self,'state_path'): atomic(self.state_path,self.state)
        except Exception as persistence_error: self.diagnostic('stop_state_save_failed',persistence_error)
        self.emit('Stopped safely at '+self.state['recovery']['phase']+'. See Recovery details and logs; job tracking is retained.')
    def show_recovery_details(self):
        messagebox.showinfo('Batch recovery',recovery_summary(self.state)+'\n\nDiagnostic log: '+str(self.diagnostic_root/'logs'/'recovery.jsonl')+'\nSaved recovery status: '+str(self.state_path.with_name('recovery_status.json')),parent=self.root)
    def reconcile_submission(self,client,job):
        self.phase='reconcile_submission'
        for index,delay in enumerate(getattr(self,'reconciliation_delays',(0,2,5))):
            if delay and self.cancel.wait(delay): raise Cancelled('Cancellation requested')
            requested=job.get('requested_prompt_id')
            if requested:
                history=self.recover('reconcile_history',lambda:client.get('/history/'+requested))
                if requested in history: return requested
            snapshot=self.recover('reconcile_queue',lambda:client.get('/queue'))
            matches=[row[1] for row in snapshot['queue_running']+snapshot['queue_pending'] if row[1]==requested or row[3].get('wan_batch_job')==job['id'] or row[3].get('client_id')==job['id']]
            if len(set(matches))==1: return matches[0]
            if len(set(matches))>1: raise AmbiguousSubmission('Multiple owned prompts found; manual reconciliation required; no resubmission')
            history=self.recover('reconcile_owned_history',lambda:client.get('/history?max_items=1000'))
            matches=[pid for pid,h in history.items() if isinstance(h.get('prompt'),list) and len(h['prompt'])>3 and (h['prompt'][3].get('wan_batch_job')==job['id'] or h['prompt'][3].get('client_id')==job['id'])]
            if len(matches)==1: return matches[0]
            if len(matches)>1: raise AmbiguousSubmission('Multiple owned history entries found; manual reconciliation required; no resubmission')
        raise AmbiguousSubmission('Submission outcome unknown; no render was resubmitted. Resume will check ownership again')
    def submit_once(self,client,job,prompt):
        if job.get('submission_post_started'): return self.reconcile_submission(client,job)
        self.phase='submit_prompt'; self.diagnostic('submission_intent_prepared')
        def rejected():
            job.setdefault('rejected_submissions',[]).append({'requested_prompt_id':job.get('requested_prompt_id'),'submitted_prompt':job.get('submitted_prompt'),'time':time.time()})
            job.update(status='pending',submission_rejected=True)
            job.pop('submission_post_started',None)
            job.pop('submitted_prompt',None); job.pop('requested_prompt_id',None); self.save()
            self.phase='workflow_validation'
            raise PermanentFailure('ComfyUI rejected this workflow. Inspect validation in ComfyUI and correct it before manual Resume')
        job['submission_post_started']=True; self.save(); self.phase='submit_prompt'; self.diagnostic('submission_post_started')
        try:
            response=client.post('/prompt',{'prompt':prompt,'prompt_id':job['requested_prompt_id'],'client_id':job['id'],'extra_data':{'wan_batch_job':job['id']}})
        except Exception as error:
            self.diagnostic('submission_response_failed',error)
            if isinstance(error,urllib.error.HTTPError) and error.code in (400,401,403,404,422): rejected()
            prompt_id=self.reconcile_submission(client,job)
            self.state['recovery']={'status':'recovered','phase':'submit_prompt (ownership reconciled)','attempt':1,'time':time.time()}; self.persist_recovery(); self.diagnostic('submission_reconciled')
            return prompt_id
        if not isinstance(response,dict): return self.reconcile_submission(client,job)
        if not isinstance(response.get('prompt_id'),str):
            if response.get('node_errors'): rejected()
            return self.reconcile_submission(client,job)
        if response.get('node_errors'): self.diagnostic('accepted_with_validation_warnings')
        self.diagnostic('submission_accepted',accepted_prompt_id=response['prompt_id'])
        return response['prompt_id']
    def locate_prompt(self,client,job):
        history=client.get('/history/'+job['prompt_id']).get(job['prompt_id'])
        if history: return history
        snapshot=client.get('/queue')
        if any(row[1]==job['prompt_id'] for row in snapshot['queue_running']+snapshot['queue_pending']): return None
        raise PromptMissing('Owned prompt absent from queue/history; resubmission is disabled')
    def download_video(self,client,entry,target):
        temporary=target.with_suffix('.part')
        with urllib.request.urlopen(client.url+'/view?'+urllib.parse.urlencode({key:entry[key] for key in ('filename','subfolder','type') if key in entry}),timeout=10) as response,open(temporary,'wb') as stream:
            count=0
            while True:
                check_cancel(); chunk=response.read(256*1024)
                if not chunk: break
                stream.write(chunk); count+=len(chunk)
            length=getattr(response,'headers',{}).get('Content-Length')
            if length and count!=int(length): raise http.client.IncompleteRead(b'',max(0,int(length)-count))
        check_cancel()
        if not count: raise PermanentFailure('Downloaded video is empty; completed render tracking retained')
        os.replace(temporary,target)
    def save_continuation_frame(self, client, job, history, video, frame):
        node=raw_frame_node(job.get('submitted_prompt',{}))
        if node is None:
            # Jobs submitted before this update have no raw frame to recover.
            self.recover('extract_legacy_final_frame',lambda:last_frame(video,frame))
            job['frame_source']='decoded_video_legacy'
            return
        entries=history.get('outputs',{}).get(node,{}).get('images',[])
        entries=[entry for entry in entries if isinstance(entry,dict) and pathlib.Path(entry.get('filename','')).suffix.lower()=='.png' and entry.get('type')=='output']
        if len(entries)!=1:
            raise PermanentFailure('Expected exactly one lossless raw final PNG. Render retained; no encoded-frame fallback or rerender was attempted')
        entry=entries[0]; staging=frame.with_name(frame.stem+'_download.png')
        self.recover('download_raw_final_frame',lambda:self.download_video(client,entry,staging))
        from PIL import Image
        try:
            with Image.open(staging) as image:
                if image.format!='PNG': raise ValueError('Not PNG')
                image.verify()
        except Exception as error:
            raise PermanentFailure('Raw final frame PNG is invalid; render tracking retained') from error
        os.replace(staging,frame)
        job.update(frame_source='raw_pre_encode_png',raw_frame_output=copy.deepcopy(entry),raw_frame_sha256=hashlib.sha256(frame.read_bytes()).hexdigest())
    def match_segment_colour(self, client, job, history, video, frame):
        node=raw_first_node(job.get('submitted_prompt',{}))
        if node is None:
            job['colour_match']={'status':'not_requested_in_submitted_workflow'}
            return video,frame
        entries=history.get('outputs',{}).get(node,{}).get('images',[])
        entries=[entry for entry in entries if isinstance(entry,dict) and pathlib.Path(entry.get('filename','')).suffix.lower()=='.png' and entry.get('type')=='output']
        if len(entries)!=1: raise PermanentFailure('Expected raw first PNG for colour matching; completed render tracking retained')
        first=frame.with_name(job['id']+'_first_raw.png')
        self.recover('download_raw_first_frame',lambda:self.download_video(client,entries[0],first))
        matched_video=video.with_name(job['id']+'_colour.mp4'); matched_frame=frame.with_name(job['id']+'_colour_last.png')
        self.phase='match_segment_colour'; self.emit('Matching segment colour; originals preserved…')
        correction=apply_colour_match(job['input_path'],first,video,frame,matched_video,matched_frame)
        correction.update(status='applied',raw_first_frame=str(first))
        job.update(original_video=str(video),original_frame=str(frame),colour_match=correction,
                   frame_source='colour_matched_raw_pre_encode_png',frame_sha256=hashlib.sha256(matched_frame.read_bytes()).hexdigest())
        return matched_video,matched_frame
    def refresh(self):
        selection=self.tree.selection()
        expanded={key:self.tree.item(key,'open') for key in self.tree.get_children()}
        self.tree.delete(*self.tree.get_children())
        self.row_preview_paths={}; self.row_images={}
        hidden=set(self.view.get('hidden_rows',[]))
        for key,jobs in groups(self.state['jobs']).items():
            parent='group:'+key; completed=sum(j['status']=='done' for j in jobs)
            joined=self.state.get('joins',{}).get(key,{}).get('status')
            active=any(tracked_work(j) for j in jobs) or joined=='joining'
            if parent in hidden and not active: continue
            visible=[j for j in jobs if 'job:'+j['id'] not in hidden or tracked_work(j)]
            group_start=render_dates(jobs[0])[0]
            group_end=render_dates(jobs[-1])[1] if all(j['status']=='done' or j.get('render_timing',{}).get('phase') in ('complete','failed','cancelled') for j in jobs) else 'Not finished'
            self.tree.insert('', 'end',iid=parent,text=jobs[0]['source'],open=expanded.get(parent,True),values=(f'{len(jobs)} total • {len(jobs)-len(visible)} hidden',f'{completed}/{len(jobs)} complete'+(f' / join {joined}' if joined else ''),group_start,group_end))
            self.row_preview_paths[parent]=jobs[0]['source']; self.tree.item(parent,image=self.placeholder)
            for j in jobs:
                if j not in visible: continue
                status=j['status']; seg=j['segment']
                input_label='Selected source image' if seg==0 else f'Last decoded frame of segment {seg}'
                if status=='pending' and seg:
                    previous=[p for p in jobs if p['segment']==seg-1]
                    status=f'Waiting for segment {seg} final frame' if not previous or previous[0]['status']!='done' else 'Ready: previous final frame'
                timing=j.get('render_timing',{})
                if 'seconds' in timing: status+=' • '+elapsed_text(j).split(' • ',1)[-1]
                start,end=render_dates(j)
                self.tree.insert(parent,'end',iid='job:'+j['id'],text=f'Segment {seg+1} of {len(jobs)}',values=(input_label,status,start,end))
                self.row_preview_paths['job:'+j['id']]=preview_input(jobs,j); self.tree.item('job:'+j['id'],image=self.placeholder)
                additions=self.state.get('group_configs',{}).get(key,{}).get('additions',[])
                if seg<len(additions) and additions[seg].strip():
                    self.tree.item('job:'+j['id'],text=f'Segment {seg+1} of {len(jobs)} — '+additions[seg].replace('\n',' ')[:65])
        self.tree.selection_set([item for item in selection if self.tree.exists(item)])
        self.update_thumbnails()
    def thumbnail_destroy(self,event):
        if event.widget is self.root:
            self.thumbnail_loader.closed.set()
            for token in list(self.after_ids):
                try: self.root.after_cancel(token)
                except tk.TclError: pass
            self.after_ids.clear()
    def schedule(self,delay,callback):
        holder={}
        def invoke():
            self.after_ids.discard(holder['token']); callback()
        holder['token']=self.root.after(delay,invoke); self.after_ids.add(holder['token'])
    def update_thumbnails(self):
        height=self.tree.winfo_height(); visible=set()
        for y in range(0,height,24):
            row=self.tree.identify_row(y)
            if row: visible.add(row)
        for old in set(self.row_images)-visible:
            if self.tree.exists(old): self.tree.item(old,image=self.placeholder)
            self.row_images.pop(old,None)
        now=time.monotonic()
        for row in visible:
            path=self.row_preview_paths.get(row)
            if not path: continue
            cached=self.thumb_cache.get(path)
            if cached:
                self.thumb_cache.move_to_end(path)
                if cached['photo'] is not None:
                    self.tree.item(row,image=cached['photo']); self.row_images[row]=cached['photo']
                else:
                    self.tree.item(row,image=self.placeholder); self.row_images.pop(row,None)
            if path not in self.thumb_pending and (not cached or now-cached['checked']> (5 if cached['photo'] else 2)):
                try: self.thumbnail_loader.requests.put_nowait((path,cached['signature'] if cached else None)); self.thumb_pending.add(path)
                except queue.Full: pass
    def thumbnail_tick(self):
        while True:
            try: path,thumb,signature=self.thumbnail_loader.results.get_nowait()
            except queue.Empty: break
            self.thumb_pending.discard(path)
            previous=self.thumb_cache.get(path)
            photo=previous['photo'] if previous and previous['signature']==signature else None
            if thumb is not None and photo is None:
                try:
                    from PIL import ImageTk
                    photo=ImageTk.PhotoImage(thumb,master=self.root)
                except Exception: pass
            self.thumb_cache[path]={'photo':photo,'signature':signature,'checked':time.monotonic()}
            self.thumb_cache.move_to_end(path)
            while len(self.thumb_cache)>128: self.thumb_cache.popitem(last=False)
        self.update_thumbnails(); self.schedule(500,self.thumbnail_tick)
    def timer_tick(self):
        jobs=self.state['jobs']
        current=next((j for j in jobs if j['id']==self.state.get('timer_job')),None)
        if current is None: current=next((j for j in jobs if j.get('status') in ('running','submitting')),None)
        if current is None: current=next((j for j in reversed(jobs) if j.get('render_timing')),None)
        if current:
            terminal=current.get('render_timing',{}).get('phase') in ('complete','failed','cancelled')
            self.timer_text.set(elapsed_text(current,verified=self.running or terminal))
        elif self.state.get('cancel_archives'):
            old=next((j for j in reversed(self.state['cancel_archives'][-1]['jobs']) if j.get('render_timing')),None)
            self.timer_text.set(elapsed_text(old,verified=True) if old else 'Render timer: idle')
        else: self.timer_text.set('Render timer: idle')
        self.schedule(1000,self.timer_tick)
    def pause(self): self.halt.set(); self.status.set('Will pause after the current segment. No interrupt sent to ComfyUI.')
    def reset_stuck(self):
        self.reset_local_requested=True; self.cancel.set(); self.halt.set()
        self.status.set('Stopping local monitoring; waiting for the worker to exit safely…')
        self.schedule(100,self.finish_reset)
    def finish_reset(self):
        if self.worker_thread is not None and self.worker_thread.is_alive():
            self.schedule(200,self.finish_reset); return
        self.running=False; self.cancel.clear(); self.halt.clear()
        self.state['reset_retry_requested']=True
        now=time.time()
        self.state['cancellation']={'status':'reset','time':now}
        self.state['worker_state']={'status':'idle','time':now}
        self.state['recovery']={'status':'idle','phase':'reset_waiting_for_restart','time':now}
        self.save(); self.persist_recovery(); self.current_job=None; self.phase='idle'
        while not self.events.empty():
            try: self.events.get_nowait()
            except queue.Empty: break
        self.reset_local_requested=False; self.refresh()
        self.recovery_text.set(recovery_summary(self.state)); self.timer_text.set('Render timer: idle')
        self.status.set('Local job reset. Completed segments preserved. When ComfyUI returns, Start / Resume checks history before retrying unfinished work.')
    def launch_worker(self, target):
        self.worker_thread=threading.Thread(target=target,daemon=True)
        self.worker_thread.start()
    def clear_queue(self):
        if self.worker_thread is not None and self.worker_thread.is_alive():
            self.status.set('Worker active. Click Pause after current and wait before Clear Queue.'); return
        try:
            count=clear_idle_queue(self.state,Client(self.state.get('url','http://127.0.0.1:8000')),self.save)
        except Exception as error:
            self.status.set('Queue retained: '+str(error)); return
        self.running=False; self.cancel.clear(); self.halt.clear(); self.current_job=None; self.phase='idle'
        # Discard messages from a finished worker so they cannot reinstate stale UI status.
        while not self.events.empty():
            try: self.events.get_nowait()
            except queue.Empty: break
        self.persist_recovery(); self.diagnostic('queue_cleared',jobs=count)
        self.refresh(); self.recovery_text.set(recovery_summary(self.state)); self.timer_text.set('Render timer: idle')
        self.status.set(f'Cleared {count} queue record(s). Records archived; all source images and output files preserved.')
    def cancel_all(self):
        self.cancel.set(); self.halt.set()
        self.status.set('Cancelling app-owned work and clearing queue; checking server ownership…')
        if not self.running:
            self.running=True
            self.launch_worker(self.finish_worker)
    def finish_worker(self):
        handled=self.cancel.is_set() and not getattr(self,'reset_local_requested',False)
        try:
            if handled: cancel_owned(self.state,Client(self.state.get('url','http://127.0.0.1:8000')),self.save,self.emit)
        except Exception as e:
            self.state['cancellation']={'status':'unconfirmed','error':str(e)}; self.save(); self.emit('Cancellation recovery retained: '+str(e))
        finally:
            MEDIA_CONTEXT.cancel=None
            self.events.put({'worker_finished':True,'cancel_handled':handled})
    def start(self):
        if self.running: return
        try:
            if any(j.get('missing_submission_details') for j in self.state['jobs']): raise ValueError('Interrupted job identity was recovered, but its submission details were lost in the reboot. Clear the queue and re-add planned work before generating. Existing media are preserved.')
            if self.state.get('cancellation',{}).get('status') in ('checking','unconfirmed'): raise ValueError('Cancellation requires reconciliation. Click Cancel render and clear queue again; no generation will be started.')
            prepare_reset_retry(self.state,Client(self.state.get('url','http://127.0.0.1:8000')),self.save)
            w,i,s=load_prompt(self.workflow.get())
            prior=self.state.get('prompt')
            if prior and prior!=w and any(j.get('prompt_id') and j['status']!='done' for j in self.state['jobs']): raise ValueError('Resume the original workflow while a prompt is outstanding.')
            # Older queues have no script: freeze their existing base with blank additions.
            for key,jobs in groups(self.state['jobs']).items():
                if key not in self.state.setdefault('group_configs',{}):
                    self.state['group_configs'][key]=script_config(prior or w,self.state.get('image_node',i) if prior else i,self.state.get('save_node',s) if prior else s,['']*len(jobs))
            self.state.update(url=self.url.get(),workflow=self.workflow.get(),output=self.output.get(),prompt=w,image_node=i,save_node=s,auto_join=self.autojoin.get(),auto_colour_match=self.autocolour.get()); self.save()
        except Exception as e: messagebox.showerror('Cannot start',str(e)); return
        self.halt.clear(); self.cancel.clear(); self.running=True; self.launch_worker(self.worker)
    def emit(self,msg): self.events.put(msg)
    def worker(self):
        if not hasattr(self,'cancel'): self.cancel=threading.Event()
        MEDIA_CONTEXT.cancel=self.cancel
        listener=None
        try:
            self.current_job=None
            self.state['worker_state']={'status':'running','time':time.time()}
            self.state['recovery']={'status':'running','time':time.time()}; self.persist_recovery(); self.diagnostic('worker_started',jobs=len(self.state['jobs']))
            c=Client(self.state['url']); out=pathlib.Path(self.state['output']); self.recover('prepare_output',lambda:out.mkdir(parents=True,exist_ok=True))
            if self.state.get('auto_join'): self.join_ready()
            for index,j in enumerate(self.state['jobs']):
                check_cancel()
                if j['status']=='done': continue
                if self.halt.is_set(): break
                self.current_job=j; self.phase='prepare_segment'; self.diagnostic('segment_selected')
                self.state['timer_job']=j['id']
                if listener: listener.close(); listener=None
                if not j.get('prompt_id') and (j['status']=='submitting' or j.get('submitted_prompt')):
                    j.update(prompt_id=self.reconcile_submission(c,j),status='running'); self.save(); self.diagnostic('submission_reconciled')
                if not j.get('prompt_id'):
                    j.setdefault('render_timing',{})['phase']='waiting'; self.save(); self.emit('Waiting to render')
                    while True:
                        check_cancel()
                        q=self.recover('wait_for_idle_queue',lambda:c.get('/queue'))
                        if not q['queue_running'] and not q['queue_pending']: break
                        self.emit('Waiting for ComfyUI to become idle; existing work is untouched.')
                        if self.halt.wait(5): check_cancel(); return
                    source=self.recover('resolve_input_frame',lambda:segment_input(self.state['jobs'],j))
                    j['input_path']=source
                    self.emit(f'Segment {j["segment"]+1}: uploading '+source)
                    check_cancel()
                    uploaded=self.recover('upload_image',lambda:c.upload(source))
                    check_cancel()
                    listener=PromptEvents(self.state['url'],j['id'])
                    with SCRIPT_LOCK:
                        self.phase='prepare_segment_prompt'; p=segment_prompt(self.state,j,uploaded)
                        j['render_timing']['phase']='submitting'
                        j.update(status='submitting',submitted_prompt=p,requested_prompt_id=str(uuid.uuid4())); self.save()
                    # Never retry this POST: a lost response may still mean accepted work.
                    j.update(prompt_id=self.submit_once(c,j,p),status='running'); self.save()
                    j['render_timing']['phase']='queued'; listener.drain(j); self.save()
                    check_cancel()
                if listener is None: listener=PromptEvents(self.state['url'],j['id'])
                self.emit('Monitoring segment '+str(index+1)+'/'+str(len(self.state['jobs']))+' — '+j['prompt_id'])
                while True:
                    check_cancel()
                    listener.drain(j)
                    self.events.put(None)
                    h=self.recover('monitor_history',lambda:c.get('/history/'+j['prompt_id'])).get(j['prompt_id'])
                    if h:
                        record_render_history(j,h); self.save()
                        self.events.put(None)
                        if h.get('status',{}).get('status_str')=='error':
                            failure={'time':time.time()}
                            for kind,data in h.get('status',{}).get('messages',[]):
                                if kind=='execution_error' and isinstance(data,dict):
                                    for key in ('node_id','node_type','exception_type'):
                                        value=str(data.get(key,''))
                                        if value and len(value)<=128 and all(character.isalnum() or character in '_.:-' for character in value): failure[key]=value
                            j.update(status='failed',failure=failure); self.save(); self.phase='render_execution'; self.diagnostic('render_execution_failed',**failure)
                            raise PermanentFailure('ComfyUI execution failed'+(' ('+failure['exception_type']+')' if failure.get('exception_type') else '')+'. Inspect its node error; this job will not be skipped or rerendered automatically')
                        if h.get('status',{}).get('completed'): break
                    else:
                        q=self.recover('monitor_queue',lambda:c.get('/queue'))
                        observe_render_queue(j,q); self.save()
                        self.events.put(None)
                        if not any(row[1]==j['prompt_id'] for row in q['queue_running']+q['queue_pending']):
                            self.recover('locate_owned_prompt',lambda:self.locate_prompt(c,j))
                    self.cancel.wait(3)
                entries=[]
                save_node=self.state.get('group_configs',{}).get(j.get('group'),{}).get('save_node',self.state['save_node'])
                for values in h.get('outputs',{}).get(save_node,{}).values():
                    if isinstance(values,list): entries.extend(x for x in values if isinstance(x,dict) and pathlib.Path(x.get('filename','')).suffix.lower() in ('.mp4','.webm','.mkv','.mov'))
                if len(entries)!=1:
                    self.phase='select_output'; raise PermanentFailure('Expected exactly one output video; inspect the selected save node. Render tracking retained')
                e=entries[0]; video=out/(j['id']+pathlib.Path(e['filename']).suffix); tmp=video.with_suffix('.part')
                j['download_partial']=str(tmp); self.save()
                if self.recover('inspect_existing_video',lambda:video.exists() and video.stat().st_size):
                    self.diagnostic('existing_video_reused')
                else: self.recover('download_video',lambda:self.download_video(c,e,video))
                j['video']=str(video); self.save()
                frames_dir=out/'frames'; frames_dir.mkdir(parents=True,exist_ok=True)
                frame=frames_dir/(j['id']+'_last.png'); j['frame_partial']=str(frame); self.save(); self.save_continuation_frame(c,j,h,video,frame)
                video,frame=self.match_segment_colour(c,j,h,video,frame)
                j.update(status='done',video=str(video),frame=str(frame),history=h); self.save(); self.phase='segment_complete'; self.diagnostic('segment_completed'); self.emit('Completed '+str(video))
                if self.state.get('auto_join'): self.join_ready()
            self.diagnostic('worker_paused' if self.halt.is_set() else 'worker_completed')
        except Cancelled: self.diagnostic('worker_cancel_requested')
        except Exception as e: self.stop_reason(e)
        finally:
            if listener: listener.close()
            if not self.cancel.is_set():
                outcome='stopped' if self.state.get('recovery',{}).get('status')=='stopped' else ('paused' if self.halt.is_set() else 'completed')
                self.state['worker_state']={'status':outcome,'time':time.time()}
                try: self.save()
                except Exception as error: self.stop_reason(error)
            self.finish_worker()
    def poll(self):
        try:
            while True:
                msg=self.events.get_nowait(); self.refresh()
                self.recovery_text.set(recovery_summary(self.state)+(' Logging is unavailable.' if self.state.get('logging_unavailable') else ''))
                if isinstance(msg,dict) and msg.get('worker_finished'):
                    if self.cancel.is_set() and not msg.get('cancel_handled') and not getattr(self,'reset_local_requested',False):
                        self.launch_worker(self.finish_worker)
                    else: self.running=False
                elif msg is not None: self.status.set(msg)
        except queue.Empty: pass
        self.schedule(300,self.poll)
    def close(self):
        if self.running:
            self.pause(); messagebox.showinfo('Work is active','Wait for this segment to finish before closing. Closing the window does not cancel ComfyUI.'); return
        self.root.destroy()

def runtime_self_test(report):
    """Offline packaging check: libraries, actual Tk window, and optional FFmpeg; no server calls."""
    import numpy as np
    from PIL import Image, ImageTk
    import websocket
    root=tk.Tk(); root.title('WAN Batch Studio packaging check'); root.update(); root.destroy()
    result={'version':VERSION,'data_directory':str(ROOT),'tk_window':True,'numpy':np.__version__,
            'pillow':Image.__version__,'websocket':websocket.__version__,'timezone':str(LONDON),
            'ffmpeg':shutil.which('ffmpeg'),'ffprobe':shutil.which('ffprobe')}
    pathlib.Path(report).write_text(json.dumps(result,indent=2),encoding='utf-8')

if __name__=='__main__':
    if len(sys.argv)==3 and sys.argv[1]=='--self-test-report':
        runtime_self_test(sys.argv[2]); raise SystemExit(0)
    import msvcrt
    lock=open(ROOT/'app.lock','a+b'); lock.seek(0); lock.write(b'0'); lock.flush(); lock.seek(0)
    try: msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
    except OSError:
        messagebox.showerror('Already running','WAN Batch Studio is already open.'); raise SystemExit(1)
    try: App(tk.Tk()).root.mainloop()
    except Exception:
        log=ROOT/'startup-error.log'; log.write_text(traceback.format_exc(),encoding='utf-8')
        messagebox.showerror('WAN Batch Studio could not start','Startup details were saved to '+str(log)); raise
