# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# <font size="3">__Loading Connections and Libraries__</font>
#
# <font size="3">
# Before anything is Run, these 4 commands need to be run consectutively (at least on a local machine) because of the way that pyspark was set up originally. Tanjin helped to create these steps
#
# source .env
# export CBH_SPARK_LOCAL=1
# export PYSPARK_SUBMIT_ARGS="--packages ${CBH_SPARK_PACKAGES} pyspark-shell"
# jupyter notebook
#
# Once these are run, a Jupyter windows opens and the files (professional and facility) can be selected and run.</font>

# +
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark import SparkContext
from cbh_setup import CityblockSparkContext

cbh_sc = CityblockSparkContext(sc=SparkContext.getOrCreate())

sc = cbh_sc.context
spark = cbh_sc.session

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
# -

##pulls in the necessary libraries and types. Numpy was used for QA
from pyspark.sql.types import LongType, ArrayType, NumericType, DecimalType, DateType, BooleanType, StructField, IntegerType, StructType, StringType, MapType, ArrayType, DoubleType, FloatType
from pyspark.sql.functions import array, col, explode, struct, lit, when, arrays_zip, concat_ws, split
from pyspark.sql import Row
import pyspark.sql.functions as F
import numpy as np
import pandas as pd

# <font size="3">__Loading MassHealth Tables from Silver__</font>
#
# <font size="3">
# The next set of loads connect to masshealth claims shards, Tufts professional claims, Tufts provider data, and tufts member data. Because Python does not allow to select shards using MassHealth_2020*, we will need to either run this process individually for each shard, and then join at then end or load all shards individually, union them, and then run everything once. The downfall of any of these options is that there is currently no flexibility to automatically pull in new shards. Another way that this can be avoided is to create a view referencing all shards, but again, Python will need to be reconfigured to reference views, because it currently cannot do so due to the initial setup of Pyspark.</font>

df_MassHealth4 = spark.read.format('bigquery').option('table', 'tufts-data:silver_claims.MassHealth_20210310').load()
df_MassHealth4.createOrReplaceGlobalTempView("global_temp_MassHealth4")

df_TuftsProv = spark.read.format('bigquery').option('table', 'tufts-data:gold_claims.Provider_20210310').load()
df_TuftsProv.createOrReplaceGlobalTempView("global_temp_TuftsProv")

df_MM = spark.read.format('bigquery').option('table', 'cityblock-analytics:mrt_member.master_member_v1').load()
df_MM.createOrReplaceGlobalTempView("global_temp_MM")

df_MassHealth_clms_prof = spark.sql('select distinct "tufts" as partner, "Medicaid" as lineOfBusiness,  cast(null as string) as sublineOfBusiness, \
cast(null as string) as commonID,  false as capitatedFlag, \
false as inNetworkFlag, data.CDE_TYPE_OF_BILL as typeOfBill, data.CDE_REVENUE as revenueCode,\
cast(null as string) as operating_prov_ID, cast(null as string) as operating_specialty, \
case when SERVICING_prov_ID is null then data.NPI_SERVICING end as SERVICING_prov_ID, data.CDE_PROV_TYPE_SERVICING as servicing_specialty, \
case when billing_prov_ID is null then data.NPI_BILLING end as  billing_prov_ID, data.CDE_PROV_TYPE_BILLING as billing_specialty, \
case when referring_prov_ID is null then data.NPI_REFERRING end as referring_prov_ID,  data.CDE_PROV_TYPE_REFERRING as referring_specialty, \
cast(0 as decimal ) as cob, false as cobFlag, data.CDE_DRG as DRG_Code, data.Surg_Proc_ICD_version as DRG_Version, cast(null as string) as DRG_Codeset,\
data.NUM_LOGICAL_CLAIM as partnerClaimID, cast(data.NUM_DTL as Long) as lineNumber, \
"tufts-data" as project, "silver_claims" as dataset,"MassHealth" as table, \
data.CDE_ADMIT_TYPE as admissionType, data.CDE_ADMIT_SOURCE as admissionSource, data.CDE_PATIENT_STATUS as dischargeStatus, \
data.CDE_DIAG_1, data.CDE_DIAG_2, data.CDE_DIAG_3, data.CDE_DIAG_4, data.CDE_DIAG_5, data.CDE_DIAG_6, data.CDE_DIAG_7, data.CDE_DIAG_8, data.CDE_DIAG_9, data.CDE_DIAG_10, data.CDE_DIAG_11, data.CDE_DIAG_12, data.CDE_DIAG_13, data.CDE_DIAG_14, data.CDE_DIAG_15, data.CDE_DIAG_16,data.CDE_DIAG_17, data.CDE_DIAG_18, data.CDE_DIAG_19, data.CDE_DIAG_20, data.CDE_DIAG_21, data.CDE_DIAG_22, data.CDE_DIAG_23, data.CDE_DIAG_24, data.CDE_DIAG_25, data.CDE_DIAG_26, \
identifier.surrogateId, pat.patientId, data.Mem_Id as partnerMemberID , \
concat(pat.patientId,data.NUM_LOGICAL_CLAIM) as claimID,\
data.DOS_FROM_DT as from, data.DOS_TO_DT as to, data.REMIT_DT as paid,  cast (null as date) as admit,   cast (null as date) as discharge, \
data.CDE_PROC , data.CDE_PROC_MOD, data.CDE_PROC_MOD_2, data.CDE_PROC_MOD_3, data.CDE_PROC_MOD_4, \
data.AMT_BILLED as billed , cast(null as float) as coinsurance , data.AMT_PAID as planPaid, cast(null as float)  as copay, data.AMT_PAID_MCARE + data.AMT_PAID as allowed,  cast(null as string) as deductible ,  cast(data.QTY_UNITS_BILLED as INT)  as serviceQuantity, \
case when data.CLAIM_STATUS = "P" then "Paid" when data.CLAIM_STATUS = "D" then "Denied" end as claimLineStatus , \
data.CDE_PLACE_OF_SERVICE as placeOfService , cast(null as string) as service_code, cast(null as string) as service_tier \
from global_temp.global_temp_MassHealth4 m4 \
inner join \
(select distinct cohortName, patientID, memberID from global_temp.global_temp_MM \
where lower(partnerName) = "tufts" and \
((cohortname in ("Tufts Cohort 1", "Tufts Cohort 2") and isCityblockMemberMonth = TRUE) or \
(cohortname is null and isCityblockMemberMonth = TRUE) \
or (cohortname = "Tufts Cohort 3" ) \
)) pat \
on m4.data.Mem_Id = pat.memberID \
left join (select distinct max(providerIdentifier.id) as billing_prov_ID, npi from global_temp.global_temp_TuftsProv group by NPI) billing \
on data.NPI_BILLING = billing.npi \
left join (select distinct  max(providerIdentifier.id) as referring_prov_ID, npi from global_temp.global_temp_TuftsProv group by NPI) ref \
on data.NPI_REFERRING = ref.npi \
left join (select distinct  max(providerIdentifier.id) as servicing_prov_ID, npi from global_temp.global_temp_TuftsProv group by NPI) serve \
on data.NPI_SERVICING = serve.npi \
left join (select distinct data.NUM_LOGICAL_CLAIM revclm , max(data.CDE_REVENUE) rev from  global_temp.global_temp_MassHealth4 group by data.NUM_LOGICAL_CLAIM order by 1) \
on data.NUM_LOGICAL_CLAIM = revclm \
left join \
(select distinct data.NUM_LOGICAL_CLAIM billclm , max(data.CDE_TYPE_OF_BILL) bill from global_temp.global_temp_MassHealth4 group by data.NUM_LOGICAL_CLAIM order by 1) \
on data.NUM_LOGICAL_CLAIM = billclm \
where (rev is not null or bill is not null) ')


# <font size="3"> __Checkpoint 1__</font>
#
# <font size="3"> Here, and in other locations in the code, we institute a series of checkpoints to make the code run faster since it is a large amount of data.</font>
#

df_MassHealth_clms_prof.write.mode('overwrite').orc('gs://cbh-rachel-hong-spark-temp/checkpoint1/')

df_MassHealth_clms_prof = spark.read.orc('gs://cbh-rachel-hong-spark-temp/checkpoint1/').dropDuplicates()


# <font size="3">__Create array and dataframe of CPT codes__</font>
#
# <font size="3"> The format must be changed from a one-cpt-per-column format to a one-row-per-cpt format. Here, we transpose the cpt data and create an array</font>

def proc_mods(df):
    cols = [col for col in df.columns if col.startswith("CDE_PROC_MOD")]
    kvs = explode(array([col(c).alias("proccode") for c in cols]))
    return df.withColumn("modifier", kvs) \
        .drop(*cols) 


newerDfpre = proc_mods(df_MassHealth_clms_prof).dropDuplicates()
newerDf = newerDfpre.select("claimID","lineNumber","modifier").where(newerDfpre.modifier.isNotNull())


# +
##Modifiers
moddf = newerDf.groupBy("claimID", "lineNumber").agg(F.collect_set(
    (
        col("modifier")
    )
).alias("modifiers"))

max_prep = newerDfpre.join(moddf,  on= ["claimID","lineNumber"], how='left').dropDuplicates()

# -

# <font size="3">__Create array and dataframe of Dxs__</font>
#
# <font size="3"> The format must be changed from a one-dx-per-column format to a one-row-per-dx format. Here, we transpose the dx data and create an array</font>

justone = max_prep.select("claimID","surrogateId", "lineNumber", "project","dataset","table","from", "to", "admit","discharge","paid","CDE_PROC").where(max_prep.lineNumber == 1).withColumnRenamed("surrogateId", "maxsurrogateid").withColumnRenamed("project", "maxproject").withColumnRenamed("dataset", "maxdataset").withColumnRenamed("table", "maxtable").withColumnRenamed("discharge", "maxdischarge").withColumnRenamed("paid", "maxpaid").withColumnRenamed("from", "maxfrom").withColumnRenamed("to", "maxto").withColumnRenamed("admit", "maxadmit").withColumnRenamed("CDE_PROC", "maxCDE_PROC").dropDuplicates()
struct_prep = max_prep.join(justone,  on= ["claimID","lineNumber"], how='left').dropDuplicates()

# <font size="3">__Create DataFrame__</font>
#
# <font size="3"> Here is the majority of the structured df we need, with substantial nesting </font>

##Create dataFrame
df_MassHealth_clms_prof_structs = struct_prep \
    .withColumn("maxdate", struct(col("maxfrom").alias("from"), col("maxto").alias("to"), col("maxadmit").alias("admit"), col("maxdischarge").alias("discharge"), col("maxpaid").alias("paid"))) \
    .withColumn("referring", struct(col("referring_prov_ID").alias("id"),col("referring_specialty").alias("specialty"))) \
    .withColumn("billing", struct(col("billing_prov_ID").alias("id"),col("billing_specialty").alias("specialty"))) \
    .withColumn("servicing", struct(col("servicing_prov_ID").alias("id"),col("servicing_specialty").alias("specialty"))) \
    .withColumn("operating", struct(col("operating_prov_ID").alias("id"),col("operating_specialty").alias("specialty"))) \
    .drop("fromDate","toDate","paidDate","admitDate","dischargeDate") \
    .drop("billing_prov_ID","billing_specialty","referring_prov_ID","referring_specialty","servicing_prov_ID","servicing_specialty","operating_prov_ID","operating_specialty") \
    .withColumn("provider", struct(col("billing"), col("referring"), col("servicing"), col("operating"))) \
    .drop("billing","referring", "servicing", "operating") \
    .withColumn("amount", struct(col("allowed").cast(DecimalType(38,9)).alias("allowed"), col("billed").cast(DecimalType(38,9)).alias("billed"), col("cob").cast(DecimalType(38,9)).alias("cob"), col("copay").cast(DecimalType(38,9)).alias("copay"),col("deductible").cast(DecimalType(38,9)).alias("deductible"),col("coinsurance").cast(DecimalType(38,9)).alias("coinsurance"),col("planPaid").cast(DecimalType(38,9)).alias("planPaid"))) \
    .drop("allowed","billed","cob","copay","deductible","coinsurance","planPaid") \
    .withColumn("surrogate", struct( col("surrogateid").alias("id"),col("project"), col("dataset"),col("table"))) \
    .dropDuplicates()\
    .withColumn("maxsurrogate", struct( col("maxsurrogateid").alias("id"),col("maxproject").alias("project"), col("maxdataset").alias("dataset"),col("maxtable").alias("table"))) \
    .dropDuplicates()\
    .drop("surrogateid","table","project","dataset") \
    .drop("maxsurrogateid","maxtable","maxproject","maxdataset") \
    .dropDuplicates()\
    .withColumn("drg", struct( col("DRG_Version").alias("version"), col("DRG_Codeset").alias("codeset"),col("DRG_Code").alias("code"))) \
    .withColumn("memberIdentifier", struct( col("commonId"),col("partnerMemberId"),col("patientId"),col("partner"))) \
    .dropDuplicates()\
    .withColumn("procedures", struct( col("surrogate") , lit("primary").alias("tier"), lit("CPT").alias("codeset"), col("CDE_PROC").alias("code"))) \
    .withColumn("procedure", 
                struct(
                    col("surrogate") , lit("primary").alias("tier"), lit("CPT").alias("codeset"), col("CDE_PROC").alias("code") ,  col("modifiers")
                )
               ).drop("modifiers").dropDuplicates()


# +
procpre = df_MassHealth_clms_prof_structs.groupBy("claimID", "lineNumber").agg(F.collect_set(
    ( struct(
    col("maxsurrogate").alias("surrogate") ,lit("primary").alias("tier"), lit("CPT").alias("codeset"),col("CDE_PROC").alias("code")
    )
)).alias("procedures1")).dropDuplicates()

proc = procpre.where(procpre.procedures1.code.isNotNull()).where(procpre.procedures1.surrogate.id.isNotNull()).dropDuplicates()

df_MassHealth_clms_prof_structs2 = df_MassHealth_clms_prof_structs.join(proc, ["lineNumber","claimID"], how ="left" ).dropDuplicates()


# -

amounts = df_MassHealth_clms_prof_structs.select("claimID", "lineNumber", "amount").dropDuplicates()


# <font size="3">__Create array and dataframe of Dxs__</font>
#
# <font size="3"> The format must be changed from a one-dx-per-column format to a one-row-per-dx format. Here, we transpose the dx data and create an array</font>

def collapse_cdes(df):
    cols = [col for col in df.columns if col.startswith("CDE_DIAG")]
    kvs = explode(array([
      struct(
          col(c).alias("code"), 
          when(lit(c) == "CDE_DIAG_1", "principal").otherwise("secondary").alias("tier")
      )
        for c in cols
    ]))
    return df.withColumn("diag", kvs) \
        .withColumn("diagnosis", struct(col("maxsurrogate").alias("surrogate"), col("diag.tier").alias("tier"), lit("ICD10Cm").alias('codeset'), col("diag.code").alias("code"))) \
        .drop(*cols) \
        .drop("diag")



newDfpre1 = collapse_cdes(df_MassHealth_clms_prof_structs2).dropDuplicates()


newDfpre = newDfpre1.where(newDfpre1.diagnosis.code.isNotNull()).where(newDfpre1.diagnosis.surrogate.id.isNotNull())

diagnoses = newDfpre.groupby("claimID").agg(F.collect_set("diagnosis").alias("diagnoses")).dropDuplicates()

filtered = newDfpre1.drop("diagnoses").dropDuplicates().drop("diagnosis").dropDuplicates()


# <font size="3">__Checkpoint 2__</font>
#
# <font size="3"> Another checkpoint for ease memory issues and dedupe</font>

filtered.write.mode('overwrite').orc('gs://cbh-rachel-hong-spark-temp/checkpoints2/')

orig_df = spark.read.orc('gs://cbh-rachel-hong-spark-temp/checkpoints2/')

# <font size="3">__Create array of service codes__</font>

services = orig_df.groupBy("claimID", "lineNumber").agg(F.collect_set(
    struct(        
        col("service_tier").cast(IntegerType()).alias("tier"),
        col("service_code").alias("code")
    )
).alias("typesOfService")).dropDuplicates()

# <font size="3">__Create lines__</font>
#
# <font size="3"> combine dfs to create lines</font>

lines_prep = orig_df.select("surrogate","claimID", "lineNumber","revenueCode", "cobFlag", "capitatedFlag", "claimLineStatus", "inNetworkFlag", "serviceQuantity", "procedure"
        ).join(services, ["lineNumber","claimID"], how ='left').dropDuplicates().join(amounts, ["claimID","lineNumber"], how ='left').dropDuplicates().dropDuplicates()


lines_df = lines_prep.groupBy("claimID").agg(F.collect_set(
    struct( 
        col("surrogate"),
        col("lineNumber"),
        col("revenueCode"),
        col("cobFlag"),
        col("capitatedFlag"),
        col("claimLineStatus"),
        col("inNetworkFlag"),
        col("serviceQuantity"),
        col("typesOfService"),
        col("procedure"),
        col("amount")
    )
).alias("lines")).dropDuplicates()

# <font size="3">__Checkpoint 3__</font>
#
# <font size="3"> Another checkpoint for ease memory issues</font>

lines_df.write.mode('overwrite').orc('gs://cbh-rachel-hong-spark-temp/checkpoint3/')

fresh_lines_df = spark.read.orc('gs://cbh-rachel-hong-spark-temp/checkpoint3/')

new_orig = orig_df.select("memberIdentifier", "claimID","partnerClaimID", "typeOfBill", "lineNumber","admissionType", "admissionSource", "dischargeStatus", "lineOfBusiness","sublineOfBusiness", "drg","provider","procedures1","procedures","maxdate"
        ).where(orig_df.lineNumber == 1).join(fresh_lines_df, "claimID").join(diagnoses, "claimID", how ="left").dropDuplicates()


# <font size="3">__Assemble the final Dataframe__</font>
#
# <font size="3"> In preparation of the final load into Big Query, the final dateframe is assembled.</font>

final_df = new_orig\
    .select(
        "claimID", 
        "memberIdentifier",
        struct(
            col("partnerClaimID"),
            col("typeOfBill"),
            col("admissionType"),
            col("admissionSource"),
            col("dischargeStatus"),
            col("lineOfBusiness"),
            col("sublineOfBusiness"),
            col("drg"),
            col("provider"),
            col("diagnoses"),
            col("procedures1").alias("procedures"),
            col("maxdate").alias("date")
        ).alias("header"),
        "lines"
    ).dropDuplicates()

# <font size="3">__Write the Information to a Table__</font>
#
# <font size="3"> The last step is to write the data to a table. Below, the data is being written to a sandbox, but could be written directly to silver of gold with the correct permissions.</font>

final_df.write.mode('overwrite').format('bigquery').options(
    **{
        'table':'tufts-data.gold_claims_incremental.MassHealth_Facility_20210310',
        'intermediateFormat':'orc',
        'mode':'overwrite'
    }
).save()


