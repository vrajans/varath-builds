# Runs the SAME SCD2 MERGE predicates as notebooks/04_dim_partner_scd2.py on a
# local Delta table via delta-rs - no Fabric or Spark needed.
#   pip install deltalake pandas pyarrow && python test_scd2_merge.py
# (delta-rs uses nanosecond timestamps internally, hence arrow_cast for 9999-12-31.)
import hashlib, pandas as pd, pyarrow as pa
from deltalake import DeltaTable, write_deltalake
import shutil, tempfile; P=tempfile.mkdtemp() + "/dim_partner"; LOW=pd.Timestamp("1900-01-01"); HIGH=pd.Timestamp("9999-12-31")
T2=["region","tier","account_manager"]
schema=pa.schema([("partner_sk",pa.int64()),("partner_id",pa.string()),("partner_name",pa.string()),
  ("region",pa.string()),("tier",pa.string()),("account_manager",pa.string()),("row_hash",pa.string()),
  ("valid_from",pa.timestamp("us")),("valid_to",pa.timestamp("us")),("is_current",pa.bool_())])
write_deltalake(P, pa.Table.from_pylist([],schema=schema))
def h(r): return hashlib.sha256("|".join("<null>" if r[c] is None else r[c] for c in T2).encode()).hexdigest()
def run(rows):
    src=pd.DataFrame(rows,columns=["partner_id","partner_name","region","tier","account_manager","effective_from"])
    src["effective_from"]=pd.to_datetime(src.effective_from)
    src=src.sort_values("effective_from").groupby("partner_id").tail(1)
    src["row_hash"]=src.apply(h,axis=1)
    cur=DeltaTable(P).to_pandas(); cur=cur[cur.is_current]
    m=src.merge(cur[["partner_id","row_hash","valid_from"]],on="partner_id",suffixes=("","_t"))
    changed=m[(m.row_hash!=m.row_hash_t)&(m.effective_from>m.valid_from)][src.columns]
    a=src.assign(merge_key=src.partner_id,_new=False); b=changed.assign(merge_key=None,_new=True)
    st=pd.concat([a,b]); st["valid_from"]=st.apply(lambda r:r.effective_from if r._new else LOW,axis=1)
    st["partner_sk"]=[abs(hash((p,str(v))))%10**9 for p,v in zip(st.partner_id,st.valid_from)]
    st["valid_from"]=st.valid_from.astype("datetime64[us]"); st["effective_from"]=st.effective_from.astype("datetime64[us]")
    (DeltaTable(P).merge(source=pa.Table.from_pandas(st,preserve_index=False),
        predicate="t.partner_id = s.merge_key AND t.is_current = true",source_alias="s",target_alias="t")
     .when_matched_update(predicate="t.row_hash <> s.row_hash AND s.effective_from > t.valid_from",
        updates={"is_current":"false","valid_to":"s.effective_from"})
     .when_not_matched_insert(updates={"partner_sk":"s.partner_sk","partner_id":"s.partner_id","partner_name":"s.partner_name",
        "region":"s.region","tier":"s.tier","account_manager":"s.account_manager","row_hash":"s.row_hash",
        "valid_from":"s.valid_from","valid_to":"arrow_cast('9999-12-31T00:00:00', 'Timestamp(Microsecond, None)')","is_current":"true"})
     .execute())
def show(t):
    d=DeltaTable(P).to_pandas().sort_values(["partner_id","valid_from"])
    print("\n#",t); print(d[["partner_id","region","tier","valid_from","valid_to","is_current"]].to_string(index=False))
run([("P1","Contoso","EMEA","Gold","Ana","2026-01-05"),("P2","Fabrikam","AMER","Silver","Raj","2026-01-05")]); show("day 1: initial load")
run([("P1","Contoso","APAC","Gold","Ana","2026-06-01"),("P2","Fabrikam","AMER","Silver","Raj","2026-06-01"),("P3","Northwind","EMEA","Gold","Li","2026-06-01")]); show("day 2: P1 moves EMEA->APAC, P2 unchanged, P3 new")
run([("P1","Contoso","LATAM","Gold","Ana","2026-03-01")]); show("day 3: OUT-OF-ORDER older record for P1 (must be ignored)")
run([("P1","Contoso","APAC","Gold","Ana","2026-06-01")]); show("day 4: same change replayed (must be idempotent)")
# point-in-time join
d=DeltaTable(P).to_pandas()
facts=pd.DataFrame({"partner_id":["P1","P1","P3","P9"],"completed_at":pd.to_datetime(["2026-02-10","2026-07-10","2026-07-01","2026-07-01"])})
r=facts.merge(d,on="partner_id",how="left"); r=r[(r.completed_at>=r.valid_from)&(r.completed_at<r.valid_to)|r.valid_from.isna()]
print("\n# point-in-time join"); print(r[["partner_id","completed_at","region"]].to_string(index=False))
