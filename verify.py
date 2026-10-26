import io, json, pathlib, subprocess, tempfile, unittest, threading, queue, time, sys
from unittest.mock import patch
from app import App, atomic, load_prompt, describe, last_frame, join_segments, probe, groups, remove_pending_groups, segment_input, positive_text_nodes, script_config, segment_prompt, cancel_owned, media_run, MEDIA_CONTEXT, Cancelled, export_script, import_script
from app import record_render_event, record_render_history, observe_render_queue, elapsed_text, PromptEvents
from app import load_queue_state, colour_offsets, apply_colour_match, raw_first_node, raw_frame_node, add_raw_final_frame, editable_segment, update_future_script, prepare_reset_retry, clear_idle_queue, hide_list_rows
from app import london_timestamp, render_dates, preview_input, load_thumbnail
from datetime import datetime, timezone
from app import PermanentFailure, AmbiguousSubmission, recovery_summary, temporary_error

class CancelClient:
    def __init__(self,running=None,pending=None):
        self.running=running or []; self.pending=pending or []; self.history={}; self.calls=[]; self.supported=True
    def row(self,pid): return [0,pid,{},{}]
    def get(self,path):
        if path=='/queue': return {'queue_running':[self.row(p) for p in self.running],'queue_pending':[self.row(p) for p in self.pending]}
        if path=='/history': return self.history
        pid=path.rsplit('/',1)[1]; return {pid:self.history[pid]} if pid in self.history else {}
    def post(self,path,data):
        self.calls.append((path,data))
        if path=='/interrupt':
            assert data['prompt_id'] in self.running
            self.running.remove(data['prompt_id']); self.history[data['prompt_id']]={'status':{'status_str':'error'}}
        if path=='/queue': self.pending=[p for p in self.pending if p not in data['delete']]
        return {}
    def targeted_interrupt_supported(self): return self.supported

def script_workflow():
    return {'p':{'class_type':'CLIPTextEncode','inputs':{'text':'BASE POSITIVE'}},
            'n':{'class_type':'CLIPTextEncode','inputs':{'text':'BASE NEGATIVE'}},
            'wan':{'class_type':'WanImageToVideo','inputs':{'positive':['p',0],'negative':['n',0]}},
            'sampler':{'class_type':'KSamplerAdvanced','inputs':{'positive':['wan',0],'negative':['wan',1]}},
            'image':{'class_type':'LoadImage','inputs':{'image':'original.png'}},
            'save':{'class_type':'SaveVideo','inputs':{'filename_prefix':'base'}}}

class Checks(unittest.TestCase):
    def test_clear_idle_queue_archives_and_persists_stale_work(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'queue.json'; media=pathlib.Path(d)/'completed.mp4'; media.write_bytes(b'preserved')
            state={'jobs':[{'id':'stale','status':'cancel_unconfirmed','prompt_id':'missing','output':str(media)}],
                   'worker_state':{'status':'running'},'cancellation':{'status':'unconfirmed'},'workflow':'SmoothMix',
                   'group_configs':{'g':{'script':'keep'}},'joins':{'g':{'output':str(media)}}}
            client=CancelClient()
            self.assertEqual(clear_idle_queue(state,client,lambda:atomic(path,state)),1)
            loaded=json.loads(path.read_text()); self.assertEqual(loaded['jobs'],[])
            self.assertEqual(loaded['worker_state']['status'],'idle'); self.assertEqual(loaded['workflow'],'SmoothMix')
            self.assertEqual(loaded['clear_archives'][0]['jobs'][0]['prompt_id'],'missing')
            self.assertEqual(media.read_bytes(),b'preserved'); self.assertEqual(client.calls,[])
    def test_clear_idle_queue_retains_work_when_busy_offline_or_save_fails(self):
        original={'jobs':[{'id':'keep','status':'running'}]}
        for client in [CancelClient(running=['unrelated']),CancelClient(pending=['unrelated'])]:
            state=json.loads(json.dumps(original))
            with self.assertRaises(ValueError): clear_idle_queue(state,client,lambda:None)
            self.assertEqual(state,original); self.assertEqual(client.calls,[])
        state=json.loads(json.dumps(original))
        def fail(): raise OSError('disk unavailable')
        with self.assertRaises(OSError): clear_idle_queue(state,CancelClient(),fail)
        self.assertEqual(state,original)
        with patch.object(CancelClient,'get',side_effect=ConnectionRefusedError):
            with self.assertRaises(ConnectionRefusedError): clear_idle_queue(state,CancelClient(),lambda:None)
        self.assertEqual(state,original)
    def test_reset_retry_reconciles_before_releasing_submission_identity(self):
        state={'reset_retry_requested':True,'jobs':[{'id':'finished','status':'done','video':'keep'},
            {'id':'retry','status':'cancel_unconfirmed','prompt_id':'old','submitted_prompt':{'keep':'audit'}}]}
        client=CancelClient()
        with patch.object(client,'get',side_effect=ConnectionRefusedError):
            with self.assertRaises(ConnectionRefusedError): prepare_reset_retry(state,client,lambda:None)
        self.assertEqual(state['jobs'][1]['prompt_id'],'old')
        client.running=['unrelated']
        with self.assertRaises(ValueError): prepare_reset_retry(state,client,lambda:None)
        client.running=[]; prepare_reset_retry(state,client,lambda:None)
        self.assertEqual(state['jobs'][0]['status'],'done'); self.assertEqual(state['jobs'][1]['status'],'pending')
        self.assertNotIn('prompt_id',state['jobs'][1]); self.assertEqual(state['jobs'][1]['reset_attempts'][0]['prompt_id'],'old')
        self.assertEqual(client.calls,[])
        state={'reset_retry_requested':True,'jobs':[{'id':'recover','status':'running','prompt_id':'success'}]}
        client.history={'success':{'status':{'status_str':'success'}}}
        prepare_reset_retry(state,client,lambda:None)
        self.assertEqual(state['jobs'][0]['prompt_id'],'success'); self.assertEqual(state['jobs'][0]['status'],'running')
    def test_edit_future_segments_while_running_and_persist(self):
        with tempfile.TemporaryDirectory() as d:
            config=script_config(script_workflow(),'image','save',['done','active','future'])
            jobs=[{'id':str(i),'group':'g','segment':i,'status':status} for i,status in enumerate(['done','running','pending'])]
            jobs[1]['prompt_id']='owned'
            state={'jobs':jobs,'group_configs':{'g':config}}
            edited=json.loads(json.dumps(config)); edited['additions'][2]='new future action'
            path=pathlib.Path(d)/'queue.json'
            self.assertEqual(update_future_script(state,'g',edited,config['additions'],lambda:atomic(path,state)),1)
            loaded=json.loads(path.read_text()); self.assertEqual(loaded['group_configs']['g']['additions'],['done','active','new future action'])
            prompt=segment_prompt(loaded,jobs[2],'next.png')
            self.assertIn('new future action',prompt['p']['inputs']['text'])
            self.assertEqual(prompt['n']['inputs']['text'],'BASE NEGATIVE')
            self.assertEqual(jobs[1]['prompt_id'],'owned')
            edited['additions'][2]='too late'; jobs[2].update(status='submitting',requested_prompt_id='reserved')
            with self.assertRaisesRegex(ValueError,'Segment 3'): update_future_script(state,'g',edited,['done','active','new future action'],lambda:None)
            self.assertEqual(state['group_configs']['g']['additions'][2],'new future action')
            self.assertFalse(editable_segment(jobs[2]))
            edited['additions'][0]='change done'
            with self.assertRaisesRegex(ValueError,'Segment 1'): update_future_script(state,'g',edited,config['additions'],lambda:None)
    def test_raw_frame_branches_before_encoder_without_changing_workflow(self):
        workflow=script_workflow()
        workflow['decode']={'class_type':'VAEDecode','inputs':{}}
        workflow['create']={'class_type':'CreateVideo','inputs':{'images':['decode',0],'fps':12}}
        workflow['save']['inputs']['video']=['create',0]
        original=json.loads(json.dumps(workflow))
        state={'group_configs':{'g':script_config(workflow,'image','save',['action'])}}
        prompt=segment_prompt(state,{'id':'owned','group':'g','segment':0},'upload.png')
        raw=raw_frame_node(prompt); self.assertIsNotNone(raw)
        select=prompt[raw]['inputs']['images'][0]
        self.assertEqual(prompt[select]['inputs'],{'image':['decode',0],'batch_index':-1,'length':1})
        self.assertEqual(prompt['create'],original['create']); self.assertEqual(workflow,original)
        self.assertEqual(prompt[raw]['inputs']['filename_prefix'],'WANBatchStudio/frames/owned_last')
        self.assertEqual(len(add_raw_final_frame(prompt,'save','owned')),len(prompt))
        workflow['save']={'class_type':'VHS_VideoCombine','inputs':{'images':['decode',0]}}
        self.assertIsNotNone(raw_frame_node(add_raw_final_frame(workflow,'save','vhs')))
    def test_raw_frame_download_preferred_and_missing_raw_stops(self):
        from PIL import Image
        with tempfile.TemporaryDirectory() as d:
            app=self.recovery_app(d); d=pathlib.Path(d); frame=d/'last.png'
            pixels=Image.new('RGB',(16,16),(131,83,41)); payload=io.BytesIO(); pixels.save(payload,format='PNG')
            workflow=script_workflow(); workflow['create']={'class_type':'CreateVideo','inputs':{'images':['decode',0]}}
            workflow['save']['inputs']['video']=['create',0]
            prompt=add_raw_final_frame(workflow,'save','raw-job'); node=raw_frame_node(prompt)
            job={'id':'raw-job','submitted_prompt':prompt}
            entry={'filename':'raw.png','subfolder':'WANBatchStudio/raw','type':'output'}
            with patch('app.urllib.request.urlopen',return_value=io.BytesIO(payload.getvalue())),patch('app.last_frame') as extract:
                app.save_continuation_frame(type('C',(),{'url':'http://fake'})(),job,{'outputs':{node:{'images':[entry]}}},d/'encoded.mp4',frame)
                extract.assert_not_called()
            self.assertEqual(frame.read_bytes(),payload.getvalue()); self.assertEqual(job['frame_source'],'raw_pre_encode_png')
            with self.assertRaises(PermanentFailure): app.save_continuation_frame(None,job,{'outputs':{}},d/'encoded.mp4',frame)
            jobs=[{'id':'previous','group':'g','segment':0,'status':'done','frame':str(frame)},
                  {'id':'next','group':'g','segment':1,'status':'pending'}]
            self.assertEqual(segment_input(jobs,jobs[1]),str(frame))
            legacy={'id':'old','submitted_prompt':{}}
            with patch('app.last_frame') as extract:
                app.save_continuation_frame(None,legacy,{},d/'old.mp4',frame); extract.assert_called_once()
            self.assertEqual(legacy['frame_source'],'decoded_video_legacy')
    def test_colour_match_neutral_media_preserves_originals_and_frame_count(self):
        import numpy as np
        from PIL import Image
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); ref=d/'ref.png'; first=d/'first.png'; last=d/'last.png'; video=d/'source.mp4'
            Image.new('RGB',(32,32),(125,104,83)).save(ref)
            Image.new('RGB',(32,32),(120,100,80)).save(first)
            Image.new('RGB',(32,32),(130,110,90)).save(last)
            original_bytes=last.read_bytes()
            raw=b''.join(np.full((32,32,3),[120+i*5,100+i*5,80+i*5],dtype=np.uint8).tobytes() for i in range(3))
            subprocess.run(['ffmpeg','-v','error','-f','rawvideo','-pix_fmt','rgb24','-s','32x32','-r','3','-i','-',
                            '-vf','scale=out_color_matrix=bt709:out_range=tv','-c:v','libx264','-crf','18','-pix_fmt','yuv420p',
                            '-colorspace','bt709','-color_range','tv','-color_trc','iec61966-2-1',str(video)],input=raw,check=True,capture_output=True)
            source_bytes=video.read_bytes(); result=d/'matched.mp4'; frame=d/'matched.png'
            correction=apply_colour_match(ref,first,video,last,result,frame)
            self.assertEqual(correction['offset_rgb'],[5,4,3]); self.assertEqual(last.read_bytes(),original_bytes)
            self.assertEqual(video.read_bytes(),source_bytes)
            self.assertEqual(np.asarray(Image.open(frame))[0,0].tolist(),[135,114,93])
            self.assertEqual(next(x for x in probe(result) if x['codec_type']=='video')['nb_read_frames'],'3')
            before=np.frombuffer(subprocess.check_output(['ffmpeg','-v','error','-i',str(video),'-frames:v','1','-f','rawvideo','-pix_fmt','rgb24','-']),np.uint8).reshape(-1,3).mean(axis=0)
            after=np.frombuffer(subprocess.check_output(['ffmpeg','-v','error','-i',str(result),'-frames:v','1','-f','rawvideo','-pix_fmt','rgb24','-']),np.uint8).reshape(-1,3).mean(axis=0)
            self.assertTrue(np.all(np.abs((after-before)-[5,4,3])<3),(before,after))
            Image.new('RGB',(32,32),(140,120,100)).save(ref)
            bounded=colour_offsets(ref,first); self.assertTrue(bounded['limited']); self.assertEqual(bounded['offset_rgb'],[16,16,16])
    def test_colour_matching_adds_first_frame_only_when_enabled(self):
        w=script_workflow(); w['create']={'class_type':'CreateVideo','inputs':{'images':['decode',0]}}; w['save']['inputs']['video']=['create',0]
        state={'prompt':w,'image_node':'image','save_node':'save'}; job={'id':'j','segment':0}
        prompt=segment_prompt(state,job,'upload'); node=raw_first_node(prompt)
        self.assertIsNotNone(node)
        select=prompt[node]['inputs']['images'][0]; self.assertEqual(prompt[select]['inputs']['batch_index'],0)
        state['auto_colour_match']=False
        self.assertIsNone(raw_first_node(segment_prompt(state,job,'upload')))
    def test_main_prompt_future_only_template_and_script_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            w=script_workflow(); config=script_config(w,'image','save',['first','later'])
            submitted=segment_prompt({'group_configs':{'g':config}},{'id':'a','group':'g','segment':0},'upload')
            state={'jobs':[{'id':'a','group':'g','segment':0,'status':'running','submitted_prompt':submitted,'prompt_id':'owned'},
                           {'id':'b','group':'g','segment':1,'status':'pending'}],'group_configs':{'g':config}}
            changed=script_config(w,'image','save',['first','later'],'p','UPDATED MAIN')
            path=pathlib.Path(d)/'queue.json'
            update_future_script(state,'g',changed,config['additions'],lambda:atomic(path,state),baseline_main='BASE POSITIVE')
            loaded=json.loads(path.read_text())
            self.assertEqual(loaded['jobs'][0]['submitted_prompt']['p']['inputs']['text'],'BASE POSITIVE\nfirst')
            future=segment_prompt(loaded,loaded['jobs'][1],'upload')
            self.assertEqual(future['p']['inputs']['text'],'UPDATED MAIN\nlater')
            self.assertEqual(future['n']['inputs']['text'],'BASE NEGATIVE'); self.assertEqual(w['p']['inputs']['text'],'BASE POSITIVE')
            export=pathlib.Path(d)/'script.json'; export_script(export,changed)
            self.assertEqual(import_script(export)['main_prompt'],'UPDATED MAIN')
            changed['main_prompt']='STALE'
            with self.assertRaisesRegex(ValueError,'Main prompt changed'): update_future_script(state,'g',changed,config['additions'],lambda:None,baseline_main='BASE POSITIVE')
            state['jobs'][1].update(status='submitting',requested_prompt_id='reserved')
            with self.assertRaisesRegex(ValueError,'All segments'): update_future_script(state,'g',changed,config['additions'],lambda:None,baseline_main='UPDATED MAIN')
    def test_colour_conversion_gradient_has_no_systematic_second_pass_darkening(self):
        import numpy as np
        from PIL import Image
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); size=128; yy,xx=np.mgrid[:size,:size]
            pixels=np.stack([50+xx,40+yy,90+(xx+yy)//4],axis=2).astype(np.uint8)
            first=d/'first.png'; reference=d/'reference.png'; last=d/'last.png'; source=d/'source.mp4'
            Image.fromarray(pixels).save(first); Image.fromarray((pixels+[8,0,5]).astype(np.uint8)).save(reference); Image.fromarray(pixels).save(last)
            subprocess.run(['ffmpeg','-v','error','-f','rawvideo','-pix_fmt','rgb24','-s','128x128','-r','12','-i','-',
                            '-vf','scale=out_color_matrix=bt709:out_range=tv','-c:v','libx264','-crf','18','-pix_fmt','yuv420p',
                            '-color_range','tv','-colorspace','bt709','-color_trc','iec61966-2-1',str(source)],input=pixels.tobytes(),check=True,capture_output=True)
            def decode(path):
                return np.frombuffer(subprocess.check_output(['ffmpeg','-v','error','-i',str(path),'-frames:v','1','-f','rawvideo','-pix_fmt','rgb24','-']),np.uint8).reshape(size,size,3).astype(float)
            before=decode(source); result=d/'corrected.mp4'
            correction=apply_colour_match(reference,first,source,last,result,d/'corrected.png')
            residual=(decode(result)-before-np.array(correction['offset_rgb'])).mean(axis=(0,1))
            self.assertTrue(np.all(np.abs(residual)<1),residual)
    def test_durable_queue_backups_recover_zeroed_startup_state(self):
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); path=d/'queue.json'
            first={'jobs':[{'id':'a','source':'keep.png','segment':0,'status':'done'}]}
            second={'jobs':[{'id':'b','source':'keep.png','segment':0,'status':'pending'}]}
            atomic(path,first); atomic(path,second)
            self.assertEqual(json.loads((d/'queue.json.bak').read_text()),first)
            damaged=b'\0'*4096; path.write_bytes(damaged)
            recovered=load_queue_state(path)
            self.assertEqual(recovered['jobs'],first['jobs']); self.assertIn('restored',recovered['startup_notice'])
            self.assertEqual(next(d.glob('queue.corrupt.*.json')).read_bytes(),damaged)
            atomic(path,recovered)
            self.assertEqual(json.loads((d/'queue.json.bak').read_text()),first)
            self.assertEqual(list(d.glob('*.tmp')),[])
    def test_corrupt_queue_without_backup_opens_safe_empty_state(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'queue.json'; path.write_bytes(b'\0'*100)
            state=load_queue_state(path)
            self.assertEqual(state['jobs'],[]); self.assertEqual(state['recovery']['status'],'stopped')
            self.assertIn('No valid automatic backup',state['startup_notice'])
            self.assertEqual(path.read_bytes(),b'\0'*100)
    def recovery_app(self,directory):
        directory=pathlib.Path(directory); app=App.__new__(App)
        app.state={'jobs':[],'output':str(directory)}; app.state_path=directory/'queue.json'; app.diagnostic_root=directory
        app.events=queue.Queue(); app.cancel=threading.Event(); app.halt=threading.Event(); app.retry_delays=(0,0); app.reconciliation_delays=(0,0,0); app.phase='idle'; app.current_job=None
        return app
    def test_recovery_backoff_bounds_and_durable_private_logs(self):
        with tempfile.TemporaryDirectory() as d:
            app=self.recovery_app(d); calls=[]
            def operation():
                calls.append(1)
                if len(calls)<3: raise TimeoutError('PRIVATE_PROMPT sk-secret should never be logged')
                return 42
            self.assertEqual(app.recover('queue_read',operation),42); self.assertEqual(len(calls),3)
            sidecar=json.loads((pathlib.Path(d)/'recovery_status.json').read_text()); self.assertEqual(sidecar['status'],'recovered')
            text=(pathlib.Path(d)/'logs'/'recovery.jsonl').read_text(); self.assertNotIn('PRIVATE_PROMPT',text); self.assertNotIn('sk-secret',text)
            records=[json.loads(line) for line in text.splitlines()]; self.assertEqual([r['event'] for r in records],['retry_scheduled','retry_scheduled','operation_recovered'])
            self.assertTrue(records[0]['trace']); self.assertIn('time_london',records[0])
            calls.clear()
            def offline(): calls.append(1); raise ConnectionRefusedError('private')
            with self.assertRaises(ConnectionRefusedError) as caught: app.recover('history_read',offline)
            self.assertEqual(len(calls),3); app.stop_reason(caught.exception)
            self.assertEqual(json.loads(app.state_path.read_text())['recovery']['status'],'stopped')
    def test_recovery_does_not_retry_permanent_errors_or_media_failure(self):
        with tempfile.TemporaryDirectory() as d:
            app=self.recovery_app(d)
            for error in (ValueError('prompt text'),subprocess.CalledProcessError(1,['ffmpeg','SECRET'],stderr=b'PRIVATE')):
                operation=unittest.mock.Mock(side_effect=error)
                with self.assertRaises(type(error)): app.recover('decode',operation)
                operation.assert_called_once()
            text=(pathlib.Path(d)/'logs'/'recovery.jsonl').read_text(); self.assertNotIn('SECRET',text); self.assertNotIn('PRIVATE',text); self.assertNotIn('prompt text',text)
            transient=subprocess.CalledProcessError(1,['ffmpeg'],stderr=b'Permission denied')
            operation=unittest.mock.Mock(side_effect=[transient,'decoded'])
            self.assertEqual(app.recover('extract_final_frame',operation),'decoded'); self.assertEqual(operation.call_count,2)
    def test_recovery_queue_save_sharing_violation_and_cancel_interrupt(self):
        import app as module
        with tempfile.TemporaryDirectory() as d:
            app=self.recovery_app(d); original=module.atomic; writes=[]
            error=PermissionError('locked'); error.winerror=32
            def save(path,value):
                if path==app.state_path:
                    writes.append(1)
                    if len(writes)==1: raise error
                return original(path,value)
            with patch('app.atomic',side_effect=save): app.save()
            self.assertEqual(len(writes),2); self.assertTrue(app.state_path.exists())
            app.retry_delays=(60,); operation=unittest.mock.Mock(side_effect=TimeoutError())
            timer=threading.Timer(.05,app.cancel.set); timer.start()
            try:
                started=time.monotonic()
                with self.assertRaises(Cancelled): app.recover('queue',operation)
                self.assertLess(time.monotonic()-started,2)
            finally: timer.cancel()
    def test_submission_lost_response_reconciles_without_second_post(self):
        with tempfile.TemporaryDirectory() as d:
            app=self.recovery_app(d); job={'id':'job','status':'submitting','requested_prompt_id':'requested','submitted_prompt':{}}
            app.state['jobs']=[job]
            class Client:
                def __init__(self): self.posts=0
                def post(self,path,data): self.posts+=1; raise TimeoutError('lost response')
                def get(self,path):
                    if path=='/queue': return {'queue_running':[[0,'accepted',{}, {'wan_batch_job':'job'}]],'queue_pending':[]}
                    return {}
            client=Client(); self.assertEqual(app.submit_once(client,job,{}),'accepted'); self.assertEqual(client.posts,1)
            client.get=lambda path: {'queue_running':[],'queue_pending':[]} if path=='/queue' else {}
            with self.assertRaises(AmbiguousSubmission): app.submit_once(client,job,{})
            self.assertEqual(client.posts,1)  # Repeated handling of this ambiguous intent only reconciles; never posts again.
            self.assertEqual(job['status'],'submitting')
            self.assertTrue(json.loads(app.state_path.read_text())['jobs'][0]['submission_post_started'])
    def test_known_validation_rejection_stops_without_retrying_post_or_logging_body(self):
        with tempfile.TemporaryDirectory() as d:
            app=self.recovery_app(d); job={'id':'job','status':'submitting','requested_prompt_id':'request','submitted_prompt':{'base':'PRIVATE'}}; app.state['jobs']=[job]
            client=unittest.mock.Mock(); client.post.return_value={'node_errors':{'2':{'message':'PRIVATE validation body'}}}
            with self.assertRaises(PermanentFailure): app.submit_once(client,job,{'base':'PRIVATE'})
            client.post.assert_called_once(); self.assertEqual(job['status'],'pending'); self.assertTrue(job['submission_rejected'])
            self.assertNotIn('submission_post_started',job); self.assertNotIn('submitted_prompt',job)
            self.assertNotIn('PRIVATE',(pathlib.Path(d)/'logs'/'recovery.jsonl').read_text())
            accepted={'id':'other','status':'submitting','requested_prompt_id':'request2','submitted_prompt':{}}
            client.reset_mock(); client.post.return_value={'prompt_id':'accepted','node_errors':{'warning':'PRIVATE'}}
            self.assertEqual(app.submit_once(client,accepted,{}),'accepted'); client.post.assert_called_once()
            self.assertTrue(accepted['submission_post_started']); self.assertIn('submitted_prompt',accepted)
    def test_reconcile_accepted_history_by_exact_job_ownership(self):
        with tempfile.TemporaryDirectory() as d:
            app=self.recovery_app(d); job={'id':'job','requested_prompt_id':'request'}
            client=unittest.mock.Mock()
            client.get.side_effect=lambda path: {'queue_running':[],'queue_pending':[]} if path=='/queue' else ({'accepted':{'prompt':[0,'accepted',{}, {'wan_batch_job':'job'}]}} if path.startswith('/history?') else {})
            self.assertEqual(app.reconcile_submission(client,job),'accepted'); client.post.assert_not_called()
    def test_download_recovery_restarts_partial_file_without_rendering(self):
        with tempfile.TemporaryDirectory() as d:
            app=self.recovery_app(d); target=pathlib.Path(d)/'video.mp4'
            class Interrupted(io.BytesIO):
                def read(self,size=-1): raise ConnectionResetError('reset')
            client=type('C',(),{'url':'http://fake'})()
            with patch('app.urllib.request.urlopen',side_effect=[Interrupted(b'bad'),io.BytesIO(b'complete-video')]) as reads:
                app.recover('download_video',lambda:app.download_video(client,{'filename':'owned.mp4'},target))
            self.assertEqual(reads.call_count,2); self.assertEqual(target.read_bytes(),b'complete-video')
    def test_worker_recovers_existing_completed_video_without_render_submission(self):
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); app=self.recovery_app(d); video=d/'owned.mkv'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=blue:s=16x16:r=3:d=1','-c:v','ffv1',str(video)],check=True,capture_output=True)
            job={'id':'owned','group':'g','segment':0,'status':'running','source':'original.png','prompt_id':'accepted'}
            app.state.update(jobs=[job],url='http://fake',prompt=script_workflow(),image_node='image',save_node='save'); app.running=True
            class Client:
                def __init__(self,url): self.reads=0
                def get(self,path):
                    self.reads+=1
                    if self.reads==1: raise TimeoutError()
                    return {'accepted':{'status':{'completed':True},'outputs':{'save':{'videos':[{'filename':'owned.mkv'}]}}}}
                def post(self,*args): raise AssertionError('Must not rerender')
            with patch('app.Client',Client),patch('app.PromptEvents'),patch('app.urllib.request.urlopen',side_effect=AssertionError('Already downloaded')): app.worker()
            self.assertEqual(job['status'],'done'); self.assertTrue(pathlib.Path(job['frame']).exists()); self.assertEqual(job['prompt_id'],'accepted')
    def test_worker_permanent_failure_stops_without_skipping_or_rerendering(self):
        with tempfile.TemporaryDirectory() as d:
            app=self.recovery_app(d)
            jobs=[{'id':'bad','segment':0,'group':'g','source':'x','status':'running','prompt_id':'accepted'},{'id':'next','segment':1,'group':'g','source':'x','status':'pending'}]
            app.state.update(jobs=jobs,url='http://fake',prompt=script_workflow(),image_node='image',save_node='save')
            class Client:
                def __init__(self,url): pass
                def get(self,path): return {'accepted':{'status':{'status_str':'error','completed':False,'messages':[['execution_error',{'prompt_id':'accepted','timestamp':123000,'exception_message':'PRIVATE PROMPT'}]]}}}
                def post(self,*args): raise AssertionError('No render submission allowed')
            with patch('app.Client',Client),patch('app.PromptEvents'): app.worker()
            self.assertEqual(jobs[0]['status'],'failed'); self.assertEqual(jobs[1]['status'],'pending')
            saved=json.loads(app.state_path.read_text()); self.assertEqual(saved['recovery']['phase'],'render_execution'); self.assertEqual(saved['worker_state']['status'],'stopped')
            self.assertNotIn('PRIVATE PROMPT',(pathlib.Path(d)/'logs'/'recovery.jsonl').read_text())
    def test_recovery_reason_restores_in_ui_from_sidecar(self):
        import app as module
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); path=d/'queue.json'; atomic(path,{'jobs':[],'recovery':{'status':'running','time':1}})
            atomic(d/'recovery_status.json',{'status':'stopped','phase':'download_video','reason':'TimeoutError','time':2})
            before=path.read_bytes()
            with patch('app.STATE',path):
                root=module.tk.Tk(); ui=App(root)
                try:
                    root.geometry('640x440'); root.update()
                    self.assertIn('Stopped at download_video',ui.recovery_text.get())
                    self.assertEqual(path.read_bytes(),before)
                    for button in ui.run_buttons.values(): self.assertLessEqual(button.winfo_rooty()-root.winfo_rooty()+button.winfo_height(),root.winfo_height())
                finally: root.destroy()
    def test_run_controls_remain_visible_at_constrained_sizes_and_scaling(self):
        import app as module
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'queue.json'; atomic(path,{'jobs':[]}); before=path.read_bytes()
            for scale in (1.333,2.0,2.666):
                with self.subTest(scale=scale),patch('app.STATE',path):
                    root=module.tk.Tk(); root.tk.call('tk','scaling',scale); ui=App(root)
                    try:
                        for width,height in ((1280,780),(900,600),(800,600),(640,440)):
                            root.geometry(f'{width}x{height}')
                            for _ in range(4): root.update()
                            for label,button in ui.run_buttons.items():
                                x=button.winfo_rootx()-root.winfo_rootx(); y=button.winfo_rooty()-root.winfo_rooty()
                                self.assertTrue(button.winfo_ismapped(),(width,height,scale,label))
                                self.assertGreater(button.winfo_width(),10)
                                self.assertGreaterEqual(x,0); self.assertGreaterEqual(y,0)
                                self.assertLessEqual(x+button.winfo_width(),root.winfo_width(),(width,height,scale,label))
                                self.assertLessEqual(y+button.winfo_height(),root.winfo_height(),(width,height,scale,label))
                            self.assertLess(ui.settings_canvas.winfo_height(),root.winfo_height())
                        self.assertEqual(path.read_bytes(),before)
                    finally: root.destroy()
    def test_visible_resume_button_dispatches_worker_without_rendering(self):
        import app as module
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); path=d/'queue.json'; workflow=d/'api.json'; atomic(workflow,script_workflow())
            atomic(path,{'jobs':[{'id':'j','group':'g','source':'synthetic.png','segment':0,'status':'pending'}],'workflow':str(workflow)})
            with patch('app.STATE',path),patch('app.threading.Thread') as thread:
                root=module.tk.Tk(); ui=App(root)
                try:
                    thread.reset_mock(); root.update(); ui.run_buttons['Start / Resume'].invoke()
                    self.assertTrue(ui.running); self.assertEqual(thread.call_args.kwargs['target'],ui.worker); thread.return_value.start.assert_called_once()
                    self.assertEqual(ui.state['jobs'][0]['status'],'pending'); self.assertNotIn('prompt_id',ui.state['jobs'][0])
                    ui.run_buttons['Start / Resume'].invoke(); thread.return_value.start.assert_called_once()
                    ui.run_buttons['Pause after current'].invoke(); self.assertTrue(ui.halt.is_set())
                    persisted=json.loads(path.read_text()); self.assertEqual(persisted['prompt'],script_workflow())
                finally: root.destroy()
    def test_london_dates_and_dst(self):
        def stamp(value): return datetime.fromisoformat(value).timestamp()
        self.assertEqual(london_timestamp(stamp('2026-10-22T12:34:56+00:00')),'22 Oct 2026 13:34:56 BST')
        self.assertEqual(london_timestamp(stamp('2026-01-22T12:34:56+00:00')),'22 Jan 2026 12:34:56 GMT')
        self.assertEqual(london_timestamp(stamp('2026-10-25T00:30:00+00:00')),'25 Oct 2026 01:30:00 BST')
        self.assertEqual(london_timestamp(stamp('2026-10-25T01:30:00+00:00')),'25 Oct 2026 01:30:00 GMT')
        for unknown in (None,float('nan'),True,'yesterday'): self.assertEqual(london_timestamp(unknown),'Unknown')
    def test_render_date_states_persistence_and_history(self):
        job={'id':'j','status':'pending','prompt_id':'own'}
        self.assertEqual(render_dates(job),('Not started','—'))
        job['status']='running'; observe_render_queue(job,{'queue_running':[],'queue_pending':[[0,'own']]})
        self.assertEqual(render_dates(job),('Not started','—'))
        record_render_event(job,'execution_start',{'prompt_id':'own','timestamp':1792670400000},now=1)
        start,end=render_dates(job); self.assertEqual(start,'22 Oct 2026 13:00:00 BST'); self.assertEqual(end,'Not finished')
        record_render_event(job,'execution_error',{'prompt_id':'own','timestamp':1792670460000},now=2)
        self.assertEqual(render_dates(job)[1],'22 Oct 2026 13:01:00 BST (failed)')
        self.assertEqual(render_dates(json.loads(json.dumps(job))),render_dates(job))
        old={'prompt_id':'own','status':'done'}; record_render_history(old,{'status':{'completed':True}})
        self.assertEqual(render_dates(old),('Unknown','Unknown'))
        record_render_history(old,{'status':{'completed':True,'messages':[['execution_start',{'prompt_id':'own','timestamp':1792670400000}],['execution_success',{'prompt_id':'own','timestamp':1792670460000}]]}})
        self.assertEqual(render_dates(old),('22 Oct 2026 13:00:00 BST','22 Oct 2026 13:01:00 BST'))
    def test_preview_inputs_do_not_reuse_wrong_frame(self):
        jobs=[{'id':str(i),'group':'g','source':'original.png','segment':i,'status':'pending'} for i in range(3)]
        self.assertEqual(preview_input(jobs,jobs[0]),'original.png'); self.assertIsNone(preview_input(jobs,jobs[1]))
        jobs[0].update(status='done',frame='segment1_end.png'); self.assertEqual(preview_input(jobs,jobs[1]),'segment1_end.png')
        self.assertIsNone(preview_input(jobs,jobs[2]))
        jobs[1].update(status='done',frame='segment2_end.png'); self.assertEqual(preview_input(jobs,jobs[2]),'segment2_end.png')
        jobs[2]['input_path']='actually_uploaded.png'; self.assertEqual(preview_input(jobs,jobs[2]),'actually_uploaded.png')
    def test_thumbnail_file_arrival_and_resize(self):
        from PIL import Image
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'later.png'; self.assertEqual(load_thumbnail(path),(None,None))
            Image.new('RGB',(800,400),(255,0,0)).save(path)
            image,signature=load_thumbnail(path); self.assertLessEqual(image.width,64); self.assertLessEqual(image.height,56); self.assertIsNotNone(signature)
            self.assertEqual(image.getpixel((0,0)),(255,0,0))
    def test_date_columns_and_live_thumbnail_ui(self):
        import app as module
        from PIL import Image
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); original=d/'original.png'; later=d/'last.png'; path=d/'queue.json'
            Image.new('RGB',(100,80),(255,0,0)).save(original)
            first={'id':'first','group':'g','segment':0,'source':str(original),'status':'done','prompt_id':'first','frame':str(later),'render_timing':{'server_start':1792670400,'server_end':1792670460,'phase':'complete'}}
            second={'id':'second','group':'g','segment':1,'source':str(original),'status':'pending'}
            atomic(path,{'jobs':[first,second]}); before=path.read_bytes()
            with patch('app.STATE',path):
                root=module.tk.Tk(); ui=App(root)
                try:
                    root.update(); self.assertEqual(tuple(ui.tree['columns']),('input','status','render_start','render_end'))
                    self.assertEqual(ui.tree.set('job:first','render_start'),'22 Oct 2026 13:00:00 BST')
                    self.assertEqual(ui.tree.set('job:first','render_end'),'22 Oct 2026 13:01:00 BST')
                    self.assertEqual(ui.row_preview_paths['group:g'],str(original)); self.assertEqual(ui.row_preview_paths['job:second'],str(later))
                    thumb,signature=load_thumbnail(original); ui.thumbnail_loader.results.put((str(original),thumb,signature)); ui.thumbnail_tick()
                    self.assertIn('job:first',ui.row_images); self.assertNotIn('job:second',ui.row_images)
                    Image.new('RGB',(80,100),(0,0,255)).save(later)
                    ui.thumb_cache.pop(str(later),None); ui.thumb_pending.discard(str(later)); ui.update_thumbnails()
                    until=time.monotonic()+3
                    while 'job:second' not in ui.row_images and time.monotonic()<until:
                        root.update(); time.sleep(.02)
                    self.assertIn('job:second',ui.row_images)
                    source=ui.thumb_cache[str(later)]; self.assertIsNone(load_thumbnail(later,source['signature'])[0])
                    ui.tree.selection_set('job:second'); ui.refresh(); self.assertEqual(ui.tree.selection(),('job:second',))
                    self.assertEqual(path.read_bytes(),before)
                finally: root.destroy()
    def test_hidden_rows_preserve_jobs_dependencies_and_protect_active(self):
        state={'jobs':[{'id':str(i),'group':'g','source':'x.png','segment':i,'status':'done' if i<4 else 'pending','frame':__file__} for i in range(6)]}
        state['jobs'] += [{'id':'live','group':'active','segment':0,'status':'running','prompt_id':'own'}, {'id':'unknown','group':'uncertain','segment':0,'status':'cancel_unconfirmed','prompt_id':'unknown'}]
        original=json.dumps(state,sort_keys=True); view={}
        removed,protected=hide_list_rows(view,state['jobs'],{'job:0','job:1','job:2','job:3','job:live','group:uncertain'})
        self.assertEqual(set(removed),{'job:0','job:1','job:2','job:3'}); self.assertEqual(set(protected),{'job:live','group:uncertain'})
        self.assertEqual(json.dumps(state,sort_keys=True),original)
        self.assertEqual(segment_input(state['jobs'],state['jobs'][4]),__file__)
        self.assertEqual(len(groups(state['jobs'])['g']),6)
    def test_remove_rows_ui_selection_context_reload_and_guards(self):
        import app as module
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); path=d/'queue.json'; original=d/'original.png'; output=d/'output.mp4'
            original.write_bytes(b'keep-source'); output.write_bytes(b'keep-render')
            jobs=[{'id':str(i),'group':'g','source':str(original),'segment':i,'status':'done','prompt_id':'done'+str(i),'video':str(output),'frame':str(original)} for i in range(4)]
            jobs += [{'id':'pending','group':'g','source':str(original),'segment':4,'status':'pending'}, {'id':'live','group':'active','source':str(original),'segment':0,'status':'running','prompt_id':'own'}]
            atomic(path,{'jobs':jobs}); initial_queue=path.read_bytes()
            with patch('app.STATE',path),patch('app.messagebox.showinfo') as info:
                root=module.tk.Tk(); ui=App(root)
                try:
                    root.update(); ui.running=True  # View-only cleanup remains safe while a worker is active.
                    ui.tree.selection_set(['job:0','job:1','job:2','job:3']); ui.refresh(); root.update()
                    self.assertEqual(len(ui.tree.selection()),4)
                    ui.tree.see('job:2'); root.update(); bbox=ui.tree.bbox('job:2'); event=SimpleNamespace(y=bbox[1]+2,x_root=20,y_root=20)
                    with patch.object(module.tk.Menu,'tk_popup') as popup: ui.context_menu(event); popup.assert_called_once()
                    self.assertEqual(set(ui.tree.selection()),{'job:0','job:1','job:2','job:3'})
                    ui.row_menu.invoke('Remove selected segment rows from list')
                    self.assertTrue(ui.tree.exists('group:g')); self.assertTrue(ui.tree.exists('job:pending'))
                    self.assertTrue(all(not ui.tree.exists('job:'+str(i)) for i in range(4)))
                    self.assertEqual(path.read_bytes(),initial_queue); self.assertEqual(len(ui.state['jobs']),6)
                    self.assertEqual(len(json.loads(ui.view_path.read_text())['hidden_rows']),4)
                    # Persisted view reload reproduces hidden children without changing the queue.
                    ui.view=json.loads(ui.view_path.read_text()); ui.refresh(); self.assertFalse(ui.tree.exists('job:1'))
                    restored_window=module.tk.Toplevel(root); restored=App(restored_window)
                    self.assertFalse(restored.tree.exists('job:1')); self.assertEqual(len(restored.state['jobs']),6); restored_window.destroy()
                    ui.tree.selection_set('job:live'); ui.remove_selected(); self.assertTrue(ui.tree.exists('job:live')); self.assertIn('protected',ui.queue_feedback.get())
                    ui.show_hidden(); root.update(); ui.tree.selection_set('job:0')
                    ui.tree.see('job:pending'); root.update(); bbox=ui.tree.bbox('job:pending'); event.y=bbox[1]+2
                    with patch.object(module.tk.Menu,'tk_popup'): ui.context_menu(event)
                    self.assertEqual(ui.tree.selection(),('job:pending',))
                    ui.remove_selected(); self.assertFalse(ui.tree.exists('job:pending')); self.assertTrue(ui.tree.exists('job:0'))
                    ui.tree.selection_set('group:g'); ui.remove_selected(); self.assertFalse(ui.tree.exists('group:g')); self.assertTrue(ui.tree.exists('group:active'))
                    ui.state['jobs'][4].update(status='running',prompt_id='now-executing'); ui.refresh()
                    self.assertTrue(ui.tree.exists('group:g')); self.assertTrue(ui.tree.exists('job:pending'))
                    ui.state['jobs'][4].update(status='pending'); ui.state['jobs'][4].pop('prompt_id'); ui.refresh()
                    ui.show_hidden(); ui.tree.selection_set('job:0'); before_view=json.dumps(ui.view,sort_keys=True)
                    with patch('app.atomic',side_effect=OSError('view write failure')): ui.remove_selected()
                    self.assertEqual(json.dumps(ui.view,sort_keys=True),before_view); self.assertTrue(ui.tree.exists('job:0')); self.assertIn('could not be saved',ui.queue_feedback.get())
                    ui.tree.selection_remove(*ui.tree.selection()); ui.remove_selected(); self.assertIn('Nothing selected',ui.queue_feedback.get())
                    self.assertEqual(path.read_bytes(),initial_queue); self.assertEqual(original.read_bytes(),b'keep-source'); self.assertEqual(output.read_bytes(),b'keep-render')
                finally: root.destroy()
    def test_timer_waits_for_own_execution_and_ignores_other_prompts(self):
        job={'id':'j','segment':0,'prompt_id':'own'}
        observe_render_queue(job,{'queue_running':[[0,'unrelated']],'queue_pending':[[1,'own']]},now=100)
        self.assertIn('Queued',elapsed_text(job,now=200))
        record_render_event(job,'execution_start',{'prompt_id':'other','timestamp':150000},now=150)
        self.assertNotIn('start_wall',job['render_timing'])
        record_render_event(job,'execution_start',{'prompt_id':'own','timestamp':200000},now=200)
        self.assertIn('00:01:05',elapsed_text(job,now=265))
    def test_timer_freezes_at_server_completion_and_next_segment_resets(self):
        job={'segment':0,'prompt_id':'own'}
        record_render_event(job,'execution_start',{'prompt_id':'own','timestamp':100000},now=100.2)
        record_render_event(job,'execution_success',{'prompt_id':'own','timestamp':165000},now=165.2)
        self.assertEqual(job['render_timing']['seconds'],65)
        self.assertEqual(elapsed_text(job,now=200),elapsed_text(job,now=9999))
        next_job={'segment':1,'prompt_id':'next'}
        observe_render_queue(next_job,{'queue_running':[],'queue_pending':[[0,'next']]},now=170)
        self.assertIn('Segment 2',elapsed_text(next_job,now=190)); self.assertIn('not started',elapsed_text(next_job,now=190))
    def test_timer_resume_and_unknown_historical_start(self):
        job={'segment':0,'prompt_id':'own'}
        observe_render_queue(job,{'queue_running':[[0,'own']],'queue_pending':[]},now=100)
        self.assertIn('start unknown',elapsed_text(job,now=160)); self.assertIn('00:01:00',elapsed_text(job,now=160))
        record_render_event(job,'execution_start',{'prompt_id':'own','timestamp':90000},now=90)
        saved=json.loads(json.dumps(job))
        self.assertIn('00:02:00',elapsed_text(saved,now=210,verified=False))
        self.assertIn('unverified',elapsed_text(saved,now=210,verified=False))
        record_render_history(saved,{'status':{'completed':True,'messages':[['execution_start',{'prompt_id':'own','timestamp':90000}],['execution_success',{'prompt_id':'own','timestamp':190000}]]}})
        self.assertEqual(saved['render_timing']['seconds'],100)
    def test_timer_failure_cancel_and_missing_timestamps(self):
        for terminal in ('execution_error','execution_interrupted'):
            j={'prompt_id':'own'}
            record_render_event(j,'execution_start',{'prompt_id':'own','timestamp':100000},now=100)
            record_render_event(j,terminal,{'prompt_id':'own','timestamp':120000},now=121)
            self.assertEqual(j['render_timing']['seconds'],20)
            self.assertEqual(elapsed_text(j,now=150),elapsed_text(j,now=10000))
        old={'prompt_id':'old'}; record_render_history(old,{'status':{'completed':True}})
        self.assertIn('duration unknown',elapsed_text(old))
    def test_timer_listener_filters_without_state_thread_races(self):
        listener=PromptEvents.__new__(PromptEvents); listener.events=queue.Queue()
        listener.events.put(({'type':'execution_start','data':{'prompt_id':'other','timestamp':1000}},1))
        listener.events.put(({'type':'execution_start','data':{'prompt_id':'own','timestamp':2000}},2))
        job={'prompt_id':'own'}; listener.drain(job)
        self.assertEqual(job['render_timing']['start_wall'],2)
    def test_tagged_colour_extraction_and_join(self):
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); clips=[]
            tags=['-color_range','tv','-colorspace','bt709','-color_trc','iec61966-2-1','-color_primaries','bt709']
            for index in range(2):
                p=d/f'{index}.mp4'; clips.append(p)
                subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','testsrc2=s=32x32:r=3:d=1','-vf','setparams=range=limited:colorspace=bt709:color_trc=iec61966-2-1:color_primaries=bt709','-c:v','libx264','-pix_fmt','yuv420p']+tags+[str(p)],check=True,capture_output=True)
            frame=d/'last.png'; last_frame(clips[0],frame)
            direct=subprocess.check_output(['ffmpeg','-v','error','-i',str(clips[0]),'-vf','select=eq(n\\,2),scale=in_color_matrix=bt709:in_range=tv:out_range=pc','-frames:v','1','-f','rawvideo','-pix_fmt','rgb24','-'])
            png=subprocess.check_output(['ffmpeg','-v','error','-i',str(frame),'-f','rawvideo','-pix_fmt','rgb24','-'])
            self.assertEqual(png,direct)
            output=d/'joined.mp4'; join_segments(clips,output)
            meta=next(s for s in probe(output) if s['codec_type']=='video')
            self.assertEqual(meta['color_transfer'],'iec61966-2-1')
            self.assertEqual(meta['color_space'],'bt709'); self.assertEqual(meta['color_range'],'tv'); self.assertEqual(meta['color_primaries'],'bt709')
    def test_script_export_import_reuse(self):
        w=script_workflow(); config=script_config(w,'image','save',['she smiles','she stands up','','waves'])
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'kept.json'; export_script(path,config); imported=import_script(path)
            self.assertEqual(imported,config)
            current=script_workflow(); current['p']['inputs']['text']='NEW BASE'; current['n']['inputs']['text']='NEW NEGATIVE'
            reused=script_config(current,'image','save',imported['additions'],'p')
            prompt=segment_prompt({'group_configs':{'g':reused}},{'id':'x','group':'g','segment':1},'upload')
            self.assertEqual(prompt['p']['inputs']['text'],'NEW BASE\nshe stands up')
            self.assertEqual(prompt['n']['inputs']['text'],'NEW NEGATIVE')
            path.write_text('{"format":"wrong","version":1}')
            with self.assertRaisesRegex(ValueError,'Unsupported'): import_script(path)
    def cancel_state(self,status='running',pid='own'):
        return {'jobs':[{'id':'appjob','group':'g','segment':0,'source':'original.png','status':status,**({'prompt_id':pid} if pid else {}),'submitted_prompt':{'saved':'ownership'}}]}
    def test_cancel_owned_and_pending_preserves_unrelated(self):
        c=CancelClient(['own'],['pending-own','unrelated-pending']); state=self.cancel_state()
        state['jobs'].append({'id':'second','group':'h','segment':0,'status':'running','prompt_id':'pending-own'})
        with tempfile.TemporaryDirectory() as d:
            output=pathlib.Path(d)/'complete.mp4'; output.write_bytes(b'preserved')
            state['jobs'].append({'id':'complete','group':'x','segment':0,'status':'done','video':str(output)})
            self.assertTrue(cancel_owned(state,c,lambda:None,lambda m:None,timeout=0))
            self.assertEqual(state['jobs'],[]); self.assertEqual(output.read_bytes(),b'preserved')
            self.assertEqual(c.pending,['unrelated-pending'])
            self.assertEqual(c.calls,[('/interrupt',{'prompt_id':'own'}),('/queue',{'delete':['pending-own']})])
            self.assertEqual(len(state['cancel_archives'][0]['jobs']),3)
    def test_cancel_unrelated_and_late_completion(self):
        c=CancelClient(['unrelated']); c.history['own']={'status':{'completed':True},'outputs':{'video':'late result'}}
        state=self.cancel_state(); self.assertTrue(cancel_owned(state,c,lambda:None,lambda m:None,timeout=0))
        self.assertEqual(c.calls,[]); self.assertEqual(c.running,['unrelated'])
        self.assertIn('outputs',state['cancel_archives'][0]['jobs'][0]['cancel_history'])
    def test_cancel_ambiguous_offline_and_unsupported(self):
        for scenario in ('ambiguous','offline','unsupported'):
            with self.subTest(scenario=scenario):
                c=CancelClient(['own']); state=self.cancel_state('submitting',None) if scenario=='ambiguous' else self.cancel_state()
                if scenario=='offline': c.get=lambda path: (_ for _ in ()).throw(OSError('offline'))
                if scenario=='unsupported': c.supported=False
                self.assertFalse(cancel_owned(state,c,lambda:None,lambda m:None,timeout=0))
                self.assertEqual(state['jobs'][0]['status'],'cancel_unconfirmed'); self.assertEqual(c.calls,[])
    def test_cancel_identity_changes_before_interrupt(self):
        c=CancelClient(['own']); original=c.get; reads=[]
        def get(path):
            if path=='/queue':
                reads.append(path)
                if len(reads)==3:
                    c.running=['unrelated']; c.history['own']={'status':{'completed':True}}
            return original(path)
        c.get=get
        self.assertTrue(cancel_owned(self.cancel_state(),c,lambda:None,lambda m:None,timeout=0))
        self.assertEqual(c.calls,[])
    def test_cancel_during_submission_starts_no_next_job(self):
        app=App.__new__(App); app.cancel=threading.Event(); app.halt=threading.Event(); app.events=queue.Queue(); app.running=True
        jobs=[{'id':str(i),'group':'g','source':'image.png','segment':i,'status':'pending'} for i in range(4)]
        with tempfile.TemporaryDirectory() as d:
            app.state={'jobs':jobs,'url':'http://fake','output':d,'prompt':script_workflow(),'image_node':'image','save_node':'save'}; app.save=lambda:None
            client=CancelClient()
            client.upload=lambda path:'uploaded.png'
            original=client.post
            def post(path,data):
                if path=='/prompt':
                    client.calls.append((path,data)); client.running=['own']; app.cancel.set(); return {'prompt_id':'own'}
                return original(path,data)
            client.post=post
            with patch('app.Client',return_value=client),patch('app.PromptEvents'): app.worker()
            self.assertEqual([path for path,_ in client.calls],['/prompt','/interrupt'])
            self.assertEqual(app.state['jobs'],[]); self.assertEqual(len(app.state['cancel_archives'][0]['jobs']),4)
    def test_local_process_cancellation(self):
        event=threading.Event(); MEDIA_CONTEXT.cancel=event
        timer=threading.Timer(.1,event.set); timer.start(); start=time.monotonic()
        try:
            with self.assertRaises(Cancelled): media_run([sys.executable,'-c','import time; time.sleep(10)'])
            self.assertLess(time.monotonic()-start,3)
        finally: timer.cancel(); MEDIA_CONTEXT.cancel=None
    def test_cancel_failure_then_retry(self):
        c=CancelClient(['own']); state=self.cancel_state()
        c.post=lambda path,data: (_ for _ in ()).throw(OSError('response lost'))
        self.assertFalse(cancel_owned(state,c,lambda:None,lambda m:None,timeout=0))
        self.assertTrue(state['jobs'][0]['cancel_observed'])
        c.running=[]  # A second read confirms the previously observed job has left the server.
        self.assertTrue(cancel_owned(state,c,lambda:None,lambda m:None,timeout=0))
        self.assertEqual(state['jobs'],[])
    def test_script_per_segment_and_resume(self):
        w=script_workflow(); original=json.dumps(w,sort_keys=True)
        config=script_config(w,'image','save',['she smiles','she stands up','','   '])
        state={'group_configs':{'g':config}}
        with tempfile.TemporaryDirectory() as d:
            f=pathlib.Path(d)/'state.json'; atomic(f,state); state=json.loads(f.read_text())
        texts=[]
        for seg in range(4):
            prompt=segment_prompt(state,{'id':str(seg),'group':'g','segment':seg},'upload.png')
            texts.append(prompt['p']['inputs']['text'])
            self.assertEqual(prompt['n'],w['n'])
        self.assertEqual(texts,['BASE POSITIVE\nshe smiles','BASE POSITIVE\nshe stands up','BASE POSITIVE','BASE POSITIVE'])
        self.assertEqual(json.dumps(w,sort_keys=True),original)
        self.assertEqual(state['group_configs']['g']['workflow'],w)
    def test_positive_discovery_and_ambiguity(self):
        w=script_workflow(); self.assertEqual(positive_text_nodes(w),['p'])
        w['second']={'class_type':'CLIPTextEncode','inputs':{'text':'SECOND'}}
        w['other']={'class_type':'KSampler','inputs':{'positive':['second',0],'negative':['n',0]}}
        with self.assertRaisesRegex(ValueError,'Choose one'): script_config(w,'image','save',['action'])
        config=script_config(w,'image','save',['action'],'second')
        p=segment_prompt({'group_configs':{'g':config}},{'id':'1','group':'g','segment':0},'x')
        self.assertEqual(p['p'],w['p']); self.assertEqual(p['second']['inputs']['text'],'SECOND\naction')
        w['other']['inputs']['negative']=['second',0]
        self.assertNotIn('second',positive_text_nodes(w))
        with self.assertRaises(ValueError): script_config(w,'image','save',['action'],'n')
    def test_four_segment_worker_chain(self):
        # Run the real queue worker and real extraction; fake only the ComfyUI transport.
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); payloads=[]
            for i in range(4):
                p=d/f'fixture{i}.mkv'
                subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i',f'color=c=0x{40+i*45:02x}{40+i*45:02x}{40+i*45:02x}:s=16x16:r=3:d=1','-c:v','ffv1',str(p)],check=True,capture_output=True)
                payloads.append(p.read_bytes())
            original=d/'original.png'; last_frame(d/'fixture0.mkv',original)
            jobs=[{'id':str(i),'group':'g','source':str(original),'segment':i,'status':'pending'} for i in range(4)]
            w=script_workflow()
            app=App.__new__(App); app.state={'jobs':jobs,'url':'http://fake','output':str(d/'outputs'),'prompt':w,'image_node':'image','save_node':'save','group_configs':{'g':script_config(w,'image','save',['smiles','stands','','waves'])}}
            app.halt=threading.Event(); app.events=queue.Queue(); app.save=lambda: None; app.running=True
            uploads=[]; prompts=[]
            class FakeClient:
                def __init__(self,url): self.url=url
                def get(self,path):
                    if path=='/queue': return {'queue_running':[],'queue_pending':[]}
                    pid=path.rsplit('/',1)[1]
                    return {pid:{'status':{'completed':True},'outputs':{'save':{'videos':[{'filename':pid+'.mkv'}]}}}}
                def upload(self,path):
                    uploads.append((path,pathlib.Path(path).read_bytes()))
                    return 'uploaded-'+str(len(uploads))
                def post(self,path,data):
                    prompts.append(data['prompt']); return {'prompt_id':str(len(prompts)-1)}
            def download(url,timeout): return io.BytesIO(payloads[int(url.split('filename=')[1].split('.mkv')[0])])
            with patch('app.Client',FakeClient),patch('app.PromptEvents'),patch('app.urllib.request.urlopen',side_effect=download): app.worker()
            self.assertTrue(all(j['status']=='done' for j in jobs),list(app.events.queue))
            self.assertEqual([p[0] for p in uploads],[str(original)]+[j['frame'] for j in jobs[:3]])
            self.assertEqual([p['image']['inputs']['image'] for p in prompts],['uploaded-1','uploaded-2','uploaded-3','uploaded-4'])
            self.assertEqual(len(set(data for _,data in uploads[1:])),3)
            self.assertEqual(jobs[3]['input_path'],jobs[2]['frame'])
            self.assertEqual([p['p']['inputs']['text'] for p in prompts],['BASE POSITIVE\nsmiles','BASE POSITIVE\nstands','BASE POSITIVE','BASE POSITIVE\nwaves'])
            self.assertTrue(all(p['n']['inputs']['text']=='BASE NEGATIVE' for p in prompts))
    def test_four_segment_worker_chain_with_lossless_raw_handoff(self):
        # Run the real queue worker and real extraction; fake only the ComfyUI transport.
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); payloads=[]; raw_payloads=[]
            from PIL import Image
            for i in range(4):
                p=d/f'fixture{i}.mkv'
                subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i',f'color=c=0x{40+i*45:02x}{40+i*45:02x}{40+i*45:02x}:s=16x16:r=3:d=1','-c:v','ffv1',str(p)],check=True,capture_output=True)
                payloads.append(p.read_bytes())
                raw=io.BytesIO(); Image.new('RGB',(16,16),(110+i*25,60+i*20,30+i*15)).save(raw,format='PNG'); raw_payloads.append(raw.getvalue())
            original=d/'original.png'; last_frame(d/'fixture0.mkv',original)
            jobs=[{'id':str(i),'group':'g','source':str(original),'segment':i,'status':'pending'} for i in range(4)]
            w=script_workflow(); w['decode']={'class_type':'VAEDecode','inputs':{}}; w['create']={'class_type':'CreateVideo','inputs':{'images':['decode',0]}}; w['save']['inputs']['video']=['create',0]
            app=App.__new__(App); app.state={'jobs':jobs,'url':'http://fake','output':str(d/'outputs'),'prompt':w,'image_node':'image','save_node':'save','group_configs':{'g':script_config(w,'image','save',['smiles','stands','','waves'])}}
            app.state['auto_colour_match']=False
            app.halt=threading.Event(); app.events=queue.Queue(); app.save=lambda: None; app.running=True
            uploads=[]; prompts=[]
            class FakeClient:
                def __init__(self,url): self.url=url
                def get(self,path):
                    if path=='/queue': return {'queue_running':[],'queue_pending':[]}
                    pid=path.rsplit('/',1)[1]
                    return {pid:{'status':{'completed':True},'outputs':{'save':{'videos':[{'filename':pid+'.mkv'}]},raw_frame_node(prompts[int(pid)]):{'images':[{'filename':pid+'.png','subfolder':'raw','type':'output'}]}}}}
                def upload(self,path):
                    uploads.append((path,pathlib.Path(path).read_bytes()))
                    return 'uploaded-'+str(len(uploads))
                def post(self,path,data):
                    prompts.append(data['prompt']); return {'prompt_id':str(len(prompts)-1)}
            def download(url,timeout):
                filename=url.split('filename=')[1].split('&')[0]; index=int(filename.split('.')[0])
                return io.BytesIO(raw_payloads[index] if filename.endswith('.png') else payloads[index])
            with patch('app.Client',FakeClient),patch('app.PromptEvents'),patch('app.urllib.request.urlopen',side_effect=download): app.worker()
            self.assertTrue(all(j['status']=='done' for j in jobs),list(app.events.queue))
            self.assertEqual([p[0] for p in uploads],[str(original)]+[j['frame'] for j in jobs[:3]])
            self.assertEqual([p['image']['inputs']['image'] for p in prompts],['uploaded-1','uploaded-2','uploaded-3','uploaded-4'])
            self.assertEqual(len(set(data for _,data in uploads[1:])),3)
            self.assertEqual(jobs[3]['input_path'],jobs[2]['frame'])
            self.assertEqual([p['p']['inputs']['text'] for p in prompts],['BASE POSITIVE\nsmiles','BASE POSITIVE\nstands','BASE POSITIVE','BASE POSITIVE\nwaves'])
            self.assertTrue(all(p['n']['inputs']['text']=='BASE NEGATIVE' for p in prompts))
            self.assertTrue(all(j['frame_source']=='raw_pre_encode_png' for j in jobs))
            self.assertEqual([data for _,data in uploads[1:]],raw_payloads[:3])

    def test_four_segment_worker_chain_with_colour_matching(self):
        # Run the real queue worker and real extraction; fake only the ComfyUI transport.
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); payloads=[]; raw_payloads=[]
            from PIL import Image
            for i in range(4):
                p=d/f'fixture{i}.mkv'
                subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i',f'color=c=0x{40+i*45:02x}{40+i*45:02x}{40+i*45:02x}:s=16x16:r=3:d=1','-c:v','ffv1',str(p)],check=True,capture_output=True)
                payloads.append(p.read_bytes())
                raw=io.BytesIO(); Image.new('RGB',(16,16),(110+i*25,60+i*20,30+i*15)).save(raw,format='PNG'); raw_payloads.append(raw.getvalue())
            original=d/'original.png'; last_frame(d/'fixture0.mkv',original)
            jobs=[{'id':str(i),'group':'g','source':str(original),'segment':i,'status':'pending'} for i in range(4)]
            w=script_workflow(); w['decode']={'class_type':'VAEDecode','inputs':{}}; w['create']={'class_type':'CreateVideo','inputs':{'images':['decode',0]}}; w['save']['inputs']['video']=['create',0]
            app=App.__new__(App); app.state={'jobs':jobs,'url':'http://fake','output':str(d/'outputs'),'prompt':w,'image_node':'image','save_node':'save','group_configs':{'g':script_config(w,'image','save',['smiles','stands','','waves'])}}
            app.state['auto_colour_match']=True; app.state['auto_join']=True
            app.halt=threading.Event(); app.events=queue.Queue(); app.save=lambda: None; app.running=True
            uploads=[]; prompts=[]
            class FakeClient:
                def __init__(self,url): self.url=url
                def get(self,path):
                    if path=='/queue': return {'queue_running':[],'queue_pending':[]}
                    pid=path.rsplit('/',1)[1]
                    return {pid:{'status':{'completed':True},'outputs':{'save':{'videos':[{'filename':pid+'.mkv'}]},raw_frame_node(prompts[int(pid)]):{'images':[{'filename':pid+'.png','subfolder':'raw','type':'output'}]},raw_first_node(prompts[int(pid)]):{'images':[{'filename':pid+'_first.png','subfolder':'raw','type':'output'}]}}}}
                def upload(self,path):
                    uploads.append((path,pathlib.Path(path).read_bytes()))
                    return 'uploaded-'+str(len(uploads))
                def post(self,path,data):
                    prompts.append(data['prompt']); return {'prompt_id':str(len(prompts)-1)}
            def download(url,timeout):
                filename=url.split('filename=')[1].split('&')[0]; index=int(filename.split('.')[0].split('_')[0])
                if filename.endswith('_first.png'):
                    import numpy as np
                    values=np.asarray(Image.open(io.BytesIO(uploads[index][1])).convert('RGB'),dtype=np.int16)-[5,4,3]
                    payload=io.BytesIO(); Image.fromarray(np.clip(values,0,255).astype(np.uint8)).save(payload,format='PNG'); payload.seek(0); return payload
                return io.BytesIO(raw_payloads[index] if filename.endswith('.png') else payloads[index])
            with patch('app.Client',FakeClient),patch('app.PromptEvents'),patch('app.urllib.request.urlopen',side_effect=download): app.worker()
            self.assertTrue(all(j['status']=='done' for j in jobs),list(app.events.queue))
            self.assertEqual([p[0] for p in uploads],[str(original)]+[j['frame'] for j in jobs[:3]])
            self.assertEqual([p['image']['inputs']['image'] for p in prompts],['uploaded-1','uploaded-2','uploaded-3','uploaded-4'])
            self.assertEqual(len(set(data for _,data in uploads[1:])),3)
            self.assertEqual(jobs[3]['input_path'],jobs[2]['frame'])
            self.assertEqual([p['p']['inputs']['text'] for p in prompts],['BASE POSITIVE\nsmiles','BASE POSITIVE\nstands','BASE POSITIVE','BASE POSITIVE\nwaves'])
            self.assertTrue(all(p['n']['inputs']['text']=='BASE NEGATIVE' for p in prompts))
            self.assertTrue(all(j['frame_source']=='colour_matched_raw_pre_encode_png' for j in jobs))
            self.assertTrue(all(j['colour_match']['offset_rgb']==[5,4,3] for j in jobs))
            self.assertTrue(all(pathlib.Path(j['original_video']).is_file() and pathlib.Path(j['original_frame']).is_file() for j in jobs))
            self.assertTrue(all(j['video'].endswith('_colour.mp4') for j in jobs))
            self.assertEqual([data for _,data in uploads[1:]],[pathlib.Path(j['frame']).read_bytes() for j in jobs[:3]])

            self.assertTrue(all(pathlib.Path(j['frame']).parent==d/'outputs'/'frames' for j in jobs))
            self.assertTrue(all(pathlib.Path(j['original_frame']).parent==d/'outputs'/'frames' for j in jobs))
            self.assertTrue(all(pathlib.Path(j['colour_match']['raw_first_frame']).parent==d/'outputs'/'frames' for j in jobs))
            joined=pathlib.Path(app.state['joins']['g']['video'])
            self.assertEqual(joined,d/'outputs'/'joined'/'g_joined.mp4'); self.assertTrue(joined.is_file())
            self.assertTrue(all(pathlib.Path(j['video']).parent==d/'outputs' for j in jobs))
            saved=joined.read_bytes(); app.join_ready(); self.assertEqual(joined.read_bytes(),saved)

    def test_safe_queue_removal(self):
        state={'jobs':[]}
        for group,status in [('pending','pending'),('active','running'),('uncertain','submitting'),('completed','done')]:
            state['jobs'] += [{'id':group+str(i),'group':group,'source':'same.png','segment':i,'status':status if i==0 else 'pending'} for i in range(4)]
        removed,protected=remove_pending_groups(state,{'active'})
        self.assertEqual(removed,[]); self.assertEqual(protected,['active'])
        removed,protected=remove_pending_groups(state)
        self.assertEqual(removed,['pending']); self.assertEqual(len(state['jobs']),12)
        self.assertEqual(set(protected),{'active','uncertain','completed'})
    def test_dependency_is_within_group(self):
        jobs=[{'id':'a','group':'a','source':'same.png','segment':0,'status':'done','frame':__file__}, {'id':'b','group':'b','source':'same.png','segment':1,'status':'pending'}]
        with self.assertRaisesRegex(RuntimeError,'immediately previous'): segment_input(jobs,jobs[1])
    def test_silent_join_and_rate_mismatch(self):
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); clips=[]
            for i,rate in enumerate((3,3,4)):
                p=d/f'{i}.mkv'; clips.append(p)
                subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i',f'color=red:s=16x16:r={rate}:d=1','-c:v','ffv1',str(p)],check=True,capture_output=True)
            output=d/'silent.mp4'; self.assertEqual(join_segments(clips[:2],output),5)
            self.assertFalse(any(s['codec_type']=='audio' for s in probe(output)))
            with self.assertRaisesRegex(ValueError,'matching constant'): join_segments([clips[0],clips[2]],d/'bad.mp4')
    def test_join_three_four_and_audio(self):
        for count in (3,4):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as d:
                d=pathlib.Path(d); clips=[]; expected=[]
                for i in range(count):
                    # The first frame duplicates the previous segment's last frame.
                    levels=[20+i*40,40+i*40,60+i*40]
                    expected.extend(levels if i==0 else levels[1:])
                    raw=b''.join(bytes([level])*16*16*3 for level in levels)
                    path=d/f'{i}.mkv'; clips.append(path)
                    args=['ffmpeg','-v','error','-f','rawvideo','-pix_fmt','rgb24','-s','16x16','-r','3','-i','-']
                    if i%2==0: args+=['-f','lavfi','-i',f'sine=frequency={400+i*100}:sample_rate=48000:duration=1','-c:a','pcm_s16le']
                    subprocess.run(args+['-c:v','ffv1',str(path)],input=raw,check=True,capture_output=True)
                output=d/'joined.mp4'; self.assertEqual(join_segments(clips,output),count*3-(count-1))
                decoded=subprocess.check_output(['ffmpeg','-v','error','-xerror','-i',str(output),'-map','0:v:0','-f','rawvideo','-pix_fmt','rgb24','-'])
                actual=[decoded[i] for i in range(0,len(decoded),16*16*3)]
                self.assertEqual(len(actual),len(expected))
                for a,b in zip(actual,expected): self.assertLess(abs(a-b),6)
                self.assertTrue(any(s['codec_type']=='audio' for s in probe(output)))
                self.assertTrue(all(p.exists() for p in clips))
    def test_group_migration(self):
        jobs=[{'id':str(i),'segment':i%3} for i in range(6)]
        self.assertEqual([len(v) for v in groups(jobs).values()],[3,3])
    def test_join_failure_retry_without_generation(self):
        app=App.__new__(App)
        with tempfile.TemporaryDirectory() as d:
            app.state={'jobs':[{'id':str(i),'group':'g','source':'image.png','segment':i,'status':'done','video':str(pathlib.Path(d)/f'{i}.mp4')} for i in range(3)]}
            app.save=lambda: None; app.emit=lambda msg: None
            with patch('app.join_segments',side_effect=RuntimeError('test failure')): app.join_ready()
            self.assertEqual(app.state['joins']['g']['status'],'error')
            with patch('app.join_segments',return_value=7) as joined: app.join_ready(); joined.assert_called_once()
            self.assertEqual(app.state['joins']['g']['status'],'done')
            self.assertTrue(all(j['status']=='done' for j in app.state['jobs']))
    def test_import_and_duration(self):
        p={'1':{'class_type':'LoadImage','inputs':{'image':'x.png'}},'2':{'class_type':'WanImageToVideo','inputs':{'length':97}},'3':{'class_type':'CreateVideo','inputs':{'fps':12}},'4':{'class_type':'SaveVideo','inputs':{}}}
        with tempfile.TemporaryDirectory() as d:
            f=pathlib.Path(d)/'p.json'; atomic(f,p)
            w,i,s=load_prompt(f); self.assertEqual((i,s),('1','4')); self.assertIn('8.08s',describe(w))
            atomic(f,{'nodes':[]})
            with self.assertRaises(ValueError): load_prompt(f)
    def test_final_decoded_frame(self):
        with tempfile.TemporaryDirectory() as d:
            d=pathlib.Path(d); video=d/'v.mkv'; frame=d/'last.png'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=red:s=16x16:r=3:d=1','-f','lavfi','-i','color=blue:s=16x16:r=3:d=1','-filter_complex','[0:v][1:v]concat=n=2:v=1:a=0','-c:v','ffv1',str(video)],check=True)
            last_frame(video,frame)
            raw=subprocess.check_output(['ffmpeg','-v','error','-i',str(frame),'-f','rawvideo','-pix_fmt','rgb24','-'])
            self.assertGreater(raw[2],200); self.assertLess(raw[0],30)

if __name__=='__main__': unittest.main()
