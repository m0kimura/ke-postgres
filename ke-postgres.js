'use strict';
const Pg=require('pg');
const keBase=require('ke-base');
module.exports=class kePostgres extends keBase {
  constructor(op, table) {
    super(op, table);
  }
  /**ok
   * データベースのオープン
   * @param  {object} op 実行オプション
   * @return {Bool}      実行結果 ok/ng true/false
   * @method
   * @override
   */
  open(op, mode) {
    let me=this, conn, rc;
    op=op||{}; op.user=op.user||'postgres'; op.psw=op.psw||'postgres';
    op.server=op.server||'localhost'; op.port=op.port||'5432'; op.db=op.db||'testdb';
    if(mode){
      conn='tcp://'+op.user+':'+op.psw+'@'+op.server+':'+op.port+'/'+op.db;
    }else{
      conn='tcp://'+op.user+':'+op.psw+'@'+op.server+':'+op.port+'/';
    }
    me.DbName=op.db;
    me.db=new Pg.Client(conn);
    me.Table=op.table;
    let wid=me.ready(); me.error='';
    me.db.connect(function(err){
      if(err){
        me.error=err; rc=false;
        console.log('postgres(ERROR):', err);
      }else{
        rc='Connect Database';
        console.log('postgres(OK)');
      }
      me.post(wid);
    });
    me.wait();
    if(!mode){
      let ss={sql: 'CREATE DATABASE '+op.db};
      rc=this.sql(ss);
    }
    return rc;
  }
  /**ok
   * SQLの実行
   * @param  {Object} op  オプション{sql: SQL文}
   * @return {Bool}       OK/NG true/false
   * @method
   */
  sql(op) {
    let me=this, wid, rc;
    me.Save=op.sql; me.error='';
    wid=me.ready();
    me.db.query(op.sql, function(err, res){
      if(err){me.error=err; rc=false;}else{rc=res;} me.post(wid, rc);
    });
    rc=me.wait(); op.res=rc; op.error=me.error;
    return rc;
  }
  create(op) {
    let sql='create table '+op.table+' ('; let tp;
    let c='', k; for(k in op.items){
      if(typeof(op.items[k])=='number'){tp='varchar('+op.items[k]+')';}
      else{
        switch(op.items[k]){
        case 'num': tp='double precision'; break;
        case 'int': tp='integer'; break;
        case 'text': tp='text'; break;
        default: tp=op.items[k];
        }
      }
      sql+=c+k+' '+tp; c=', ';
    }
    sql+=', PRIMARY KEY(';
    c=''; for(let i in op.keys){sql+=c+op.keys[i]; c=', ';} sql+='));';
    return this.sql(sql);
  }
  /**
   * テーブルを初期化
   * @param  {string} table テーブルID
   * @return {Bool}         結果
   * @method
   */
  clear (table){return this.sql('truncate table '+table);}
  /**
   * インデックスの追加
   * @param  {string} table  テーブルID
   * @param  {string} name   インデックス名
   * @param  {Array}  keys   キー項目
   * @param  {Bool} unique   重複フラグ
   * @return {Bool}          エッカ
   * @method
   */
  index (table, name, keys, unique){
    if(unique){unique='unique ';}else{unique='';}
    let sql='ceate '+unique+'index '+name+' on '+table+' using btree (';
    let c=''; for(let i in keys){sql+=c+keys[i];} sql+=');';
    return this.sql(sql);
  }
  items(op) {
    let me=this, rc; op.Data=[]; op.error='';
    op.sql='select column_name as itemname, udt_name as type, character_maximum_length as width ';
    op.sql+='from information_schema.columns where table_name = \''+op.table;
    op.sql+='\' order by ordinal_position';
    rc=me.sql(op);
    return rc;
  }
  listTables(ss, scn) {
    if(ss){scn=ss.scn;}else{scn=ss; ss={};}
    scn=scn||'public'; ss.Sql='select * from pg_tables where schemaname = \''+scn+'\' order by tablename';
    return this.query(ss);
  }
  listDbs() {
    return this.query('select * from pg_database');
  }
  primary(table, db) {
    if(!db){db=this.DbName;} if(!table){table=this.Table;}
    let sql='select ccu.column_name as COLUMN_NAME';
    sql+=' from information_schema.table_constraints tc,information_schema.constraint_column_usage ccu';
    sql+=' where tc.table_catalog=\''+db+'\'';
    sql+=' and tc.table_name=\''+table+'\'';
    sql+=' and tc.constraint_type=\'PRIMARY KEY\'';
    sql+=' and tc.table_catalog=ccu.table_catalog';
    sql+=' and tc.table_schema=ccu.table_schema';
    sql+=' and tc.table_name=ccu.table_name';
    sql+=' and tc.constraint_name=ccu.constraint_name';
    let rc=this.sql(sql); let out={};
    for(let i in rc.rows){out[rc.rows[i].column_name]='';}
    return out;
  }
  /**
   * DBのバックアップ
   * @param  {object} op 実行オプション
   * @return {Bool}      結果
   * @method
   */
  backup(op) {
    let me=this; op=op||{}; op.spec=op.spec||'db';
    if(op.object.substr(0,3)=='tb_'){op.spec='table';}
    if(op.spec=='db'){op.object=op.object||me.DbName;}
    op.path=op.path||'./'; op.to=op.to||op.spec+'_'+op.object+'_'+me.today()+'.pg';
    switch(op.spec){
    case 'db':
      me.shell('pg_dump '+op.object+' > '+op.path+op.to);
      break;
    case 'table':
      me.shell('pg_dump '+me.DbName+' -t '+op.object+' > '+op.path+op.to);
      break;
    }
  }
  offline(op) {
    let me=this; op=op||{}; op.path=op.path||'./';
    op.dbpath=op.dbpath||'/var/lib/postgresql/9.3/main/';
    //    me.shell('sudo /etc/init.d/postgresql stop');
    let cmd='tar zcvf '+op.path+'coldbackup_'+me.today()+'.tar.gz '+op.dbpath;
    me.shell(cmd);
    //    me.shell('sudo /etc/init.d/postgresql start');
  }
  online(op) {
    let me=this; op=op||{}; op.path=op.path||'~/tmp/backup_postgres/data';
    op.dbpath=op.dbpath||'/var/lib/postgresql/9.3/main/';
    me.sql('select pg_start_backup(\'date'+me.today()+'\')');
    let cmd='rsync -av --delete --exclude=pg_xlog --exclude=postmaster.pid';
    cmd+=' '+op.dbpath+'* '+op.path;
    me.shell(cmd);
    me.sql('SELECT pg_stop_backup()');
  }
  /**
   * リカバリー
   * @param  {[type]} op [description]
   * @return {[type]}    [description]
   * @method
   */
  recover(op) {
    let me=this; op=op||{}; op.spec=op.spec||'db'; if(op.spec=='db'){op.object=me.DbName;}
    op.path=op.path||'./'; me.error='';
    if(!op.from){me.error='from file not defined'; return false;}
    me.shell('pg_dump -Fc '+op.object+' > '+op.path+op.from);
  }
  /**
   * csvファイルをテーブルにロード
   * @param  {object} op オプション
   * @return {[type]}    [description]
   * @method
   */
  loadCsv(op) {
    op=op||{}; op.csv=op.csv||'load.csv';
    this.getCsv(op.csv);
    let rc, n=0; this.REC.forEach(function(rec){
      rc=this.put(op.table, [rec]); if(!rc){console.log(this.error); return rc;} n++;
    });
    console.log('output '+n); return true;
  }
  escape(txt) {
    let i, out='';
    for(i in txt){
      switch(txt[i]){
      case '\'': out+='\\'+txt[i]; break;
      case '\\': out+='\\'+txt[i]; break;
      default: out+=txt[i];
      }
    }
    return out;
  }
  /**
   * データベースクローズ
   * @return {Void} none
   * @method
   */
  close() {
    this.db.end(); return 'Closed';
  }
};
