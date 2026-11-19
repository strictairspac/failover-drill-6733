/** @typedef {{id:number,name:string,role:'leader'|'follower',term:number,log_position:number,available:boolean,fenced:boolean,vote_enabled:boolean}} Node */
/** @typedef {{id:number,name:string,fail_primary:boolean,fence_primary:boolean,max_data_loss:number,required_term:number}} Scenario */
/** @typedef {{id:number,scenario_id:number,label:string,status:string,snapshot:object,result:object|null}} Drill */
/** @typedef {{id:string,order:string,text:string,term:number,lsn:number}} StreamChange */
/** @typedef {{name:string,paused:boolean,applied:StreamChange[],applied_ids:string[],retracted:string[],next_lsn:number,retract_queue:StreamChange[],awaiting:object|null}} StreamSubscriber */
/** @typedef {{id:number,name:string,scenario_id:number,phase:'streaming'|'promoted',state:object,created_at:string}} Stream */
export {};
