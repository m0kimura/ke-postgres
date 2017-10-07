'use strict';
/* global FIBERS */
const Js=require('json-sql');
const QT=String.fromCharCode(0x27);
module.exports=class keBase{
  constructor(op, table) {
    this.Event={};
    this.DbName=''; this.Sql=''; this.REC=[];
    if(op.db){
      if(table=='initialize'){
        this.Mode='initialize';
        this.open(op, false);
        return;
      }
    }else{
      this.db=op;
    }
    if(table){
      op.table=table; this.Mode='fixed';
    }else{
      op.table=''; this.Mode='universal';
    }
    this.open(op, true);
  }
  open(){console.log('this sould be overrided.');}
  sql(){console.log('this sould be overrided.');}
  /**ok
   * SQLによる検索
   * @param  {String}  op  SQL文 this.RECにレコードオブジェクト配列を作成
   * @return {Integer}     件数
   * @method
   */
  query(op) { // 照会(SQL文)=>件数 this->rec結果配列[n][name]
    let me=this, rc, i, ss={sql: ''};
    if(typeof(op)=='string'){ss.sql=op;}else{ss.sql=op.sql;}
    rc=me.sql(ss); me.REC=[];
    if(rc){for(i in rc.rows){me.REC[i]=rc.rows[i];} rc=rc.rows.length;}
    if(typeof(op)=='object'){op.rec=me.REC; op.count=rc;}
    return rc;
  }
  jquery(json) {
    let op={}; op.sql=Js.build(json); return this.query(op);
  }
  /**ok
   * データベースオブジェクトの取得
   * @return {Object} データベース
   * @property
   */
  get database(){return this.db;}
  /**ok
   * 読み込み
   * @param  {Object} op    オプション({keys: {}, items: []}) =>
   * @return {Object}       結果データ
   * @method
   */
  read(op) {
    let a, ln, y, c, k, i, rc;
    if(this.Mode=='fixed'){op.table=this.Table;}
    if(!op.items){op.items=[]; op.items[0]='*';}
    ln=''; a=''; for(k in op.keys){ln+=a+' '+k+' = \''+op.keys[k]+'\''; a=' and';}
    y=''; c=''; for(i in op.items){y+=c+' '+op.items[i]; c=',';}
    i=0;
    let sql='select '+y+' from '+op.table; if(ln){sql+=' where '+ln+';';}
    rc=this.query(sql); this.Keys=op.keys;
    if(this.Mode=='universal'){op.rec=this.REC;}
    return rc;
  }
  /**
   * 更新
   * @return {[type]}     [description]
   * @method
   */
  rewrite() { // 再書込()=>結果
    let me=this, ss={};
    ss.table=me.Table; ss.keys=me.Keys; ss.rec=[]; ss.rec[0]=this.REC[0];
    return this.update(ss);
  }
  /**
   * 更新
   * @param  {object} op    更新オプション{table, keys{}, rec[{}]}
   * @param  {array}  cruds 操作フラグ'u'のみ更新、省略時は全件
   * @return {Bool}         Ok/NG
   * @method
   */
  update(op, cruds) { // 再書込()=>結果
    let me=this, c, k, ln, a, sql, i, rc=true;
    sql='update '+op.table+' set '; c=''; op.conditions=[];
    for(i in op.rec){if(!cruds || cruds[i]=='u'){
      for(k in op.rec[i]){sql+=c+k+' = \''+me.escape(op.rec[i][k])+'\''; c=', ';}
      ln=''; a=''; for(k in op.keys){ln+=a+' '+k+' = \''+op.keys[k]+'\''; a=' and';}
      sql+=' where '+ln+';'; op.sql=sql;
      op.conditions[i]=this.sql(op);
      if(!op.conditions[i]){rc=false;}
    }}
    return rc;
  }
  /**ok
   * 追加
   * @return {Bool}      結果 OK/NG true/false
   * @method
   */
  insert(rt) { // 追加()=>結果
    rt=rt||'';
    let op={rec: [], table: this.Table, rt: rt};
    op.rec=this.REC;
    return this.put(op);
  }
  /**ok
   * 追加
   * @param  {object} op    追加オプション{table, keys{}, rec[{}]}
   * @param  {Object} cruds 操作オプション'c'のみ追加、省略時は全件
   * @return {Bool}         結果 OK/NG true/false
   * @method
   */
  put(op, cruds) {
    let me=this, sql, c, k, rc, i;
    sql=''; c='';
    for(i in op.rec){if(!cruds || cruds[i]=='c'){
      sql='insert into '+op.table+' ('; c='';
      for(k in op.rec[i]){sql+=c+k; c=', ';}
      sql+=') values ('; c='';
      for(k in op.rec[i]){sql+=c+QT+me.escape(op.rec[i][k])+QT; c=', ';}
      sql+=') ';
      if(op.rt){sql+=' returning '+op.rt;}
      op.sql=sql;
      rc=this.sql(op); if(!rc){return false;} return rc;
    }}
    return true;
  }
  /**
 * 削除
 * @param  {object} op    削除オプション {table, keys{}}
 * @param  {array}  cruds 操作フラグ c,r,u,d,省略時すべて
 * @return {Bool}         結果 OK/NG true/false
 * @method
 */
  erase(op, cruds) { // 再書込()=>結果
    let ln, sql, a, k, i, rc=true;
    op.conditions=[];
    for(i in op.rec){if(!cruds || cruds[i]=='d'){
      ln=''; a=''; for(k in op.keys){ln+=a+k+' = \''+op.keys[k]+'\''; a=' and ';}
      sql='delete from '+op.table+' where '+ln;
      op.sql=sql;
      op.conditions[i]=this.sql(op);
      if(!op.conditions){rc=false;}
    }}
    return rc;
  }
  /**
   * 削除 this.Keysを削除
   * @return {Bool} OK/NG
   * @method
   */
  delete() {
    let ss={};
    ss.rec=[]; ss.rec[0]=this.REC[0]; ss.keys=this.Keys; ss.table=this.Table;
    return this.erase(ss);
  }
  /**
    * テーブル削除
    * @param  {string} table テーブルID
    * @return {Bool}         結果
    * @method
    */
  drop(table) {return this.sql('drop table '+table);}
  /**
   * トランザクションの開始
   * @return {Bool} 結果
   * @method
   */
  begin() {return this.sql('begin;');}
  /**
   * トランザクションの完了
   * @return {Bool} 結果
   * @method
   */
  commit() {return this.sql('commit;');}
  /**
   * トランザクションの取り消し
   * @return {Bool} 結果
   * @method
   */
  rollback() {return this.sql('rollback;');}
  /**
   * セッションルーチンの手続き
   * @param  {Function} proc セッションルーチン
   * @method
   */
  PROC(proc) {
    FIBERS(function(me){
      proc(me, this);
    }).run(this);
  }
  /**
 * 逐次制御開始
 * @return {Integer} 監視番号
 * @method
 */
  ready() {
    var id=Math.random();
    this.Event[id]=FIBERS.current;
    return id;
  }
  /**
 * 逐次制御待ち合わせ
 * @return {Anything} 待ち合わせ解除時引き渡し情報
 * @method
 */
  wait() {
    var rc=FIBERS.yield();
    return rc;
  }
  /**
 * 逐次制御解除
 * @param  {Integer}  id 監視番号
 * @param  {Anything} dt 引き渡しデータ
 * @return {Void}        none
 * @method
 */
  post(id, dt) {this.Event[id].run(dt); delete this.Event[id];}
  /**
 * 時間待ち合わせ
 * @param  {Integer} ms 待ち合わせ時間（ミリ秒）
 * @return {Void}       none
 * @method
 */
  sleep(ms) {
    let wid=this.ready();
    setTimeout(() => {this.post(wid);}, ms);
    this.wait();
  }
  /**
   * データをレコードにセット
   * @param  {Object} data  設定対象データ[{item: value}]
   * @param  {Array} items  対象項目[itemname] 省略時は全件
   * @param  {Function} fn  編集条件
   * @method
   */
  set(data, items, fn, ix) {
    let dt=[], i, j, k, it={};
    if(data instanceof Array){dt=data;}else{dt[0]=data;}
    it=this.arrangeItems(items);
    if(!fn){fn=()=>{return true;};}
    if(ix>0){
      if(items){
        for(j in it){this.REC[ix][j]=dt[0][it[j]];}
      }else{
        for(k in dt[i]){
          this.REC[ix][k]=dt[0][k];
        }
      }
    }else{
      for(i in data){
        if(fn(this.REC[i], data, i)){
          if(items){
            for(j in it){this.REC[i][j]=dt[i][it[j]];}
          }else{
            for(k in dt[i]){
              this.REC[i][k]=dt[i][k];
            }
          }
        }
      }
    }
  }
  /**
   * 項目を複写
   * @param  {Array} items 複写項目
   * @return {Void}   none
   * @method
   */
  dupe(items) {
    let i, j;
    for(i in this.REC){if(i>0){
      for(j in items){this.REC[i][items[j]]=this.REC[0][items[j]];}
    }}
  }
  /**
   * テーブル参照付け込み
   * @param  {Object} obj テーブルオブジェクト
   * @param  {Object} op  付け込みオプション{keys:{}, items:{}}
   * @return {Bool}       OK/NG
   * @method
   */
  referFrom(obj, op) {
    let i, j, ss={};
    //for(k in op.items){items.push(k);}
    for(i in this.REC){
      ss.keys={};
      for(j in op.keys){ss.keys[j]=this.REC[i][op.keys[j]];}
      obj.query(ss); this.set(obj.REC[0], op.items, '', i);
    }
  }
  edit(fn, a, b, c) {
    let i;
    for(i in this.REC){
      this.REC[i]=fn(this.REC[i], a, b, c);
    }
  }
  pull(fn, items) {
    let out=[], o, i, k, it;
    it=this.arrangeItems(items);
    if(!fn){fn=()=>{return true;};}
    for(i in this.REC){
      if(fn(this.REC[i])){
        o={};
        if(items){for(k in it){o[it[k]]=this.REC[i][k];}}
        else{o=this.REC[i];}
        out.push(o);
      }
    }
    return out;
  }
  arrangeItems(items) {
    let i, k, it={};
    if(items instanceof Array){
      for(i in items){it[items[i]]=items[i];}
    }else{
      for(k in items){if(!items[k]){it[k]=k;}else{it[k]=items[k];}}
    }
    return it;
  }
};
