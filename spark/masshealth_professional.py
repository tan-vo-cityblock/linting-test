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
##Loads up the spark session and contexts needed. This was also based on steps that were developed b
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
from pyspark.sql.types import LongType, ArrayType, NumericType, DateType,DecimalType, BooleanType, StructField, IntegerType, StructType, StringType, MapType, ArrayType, DoubleType, FloatType
from pyspark.sql.functions import array, col, explode, struct, lit, when, arrays_zip, concat_ws, split
from pyspark.sql import Row
import pyspark.sql.functions as F
import numpy as np

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

# <font size="3"> Next, we use spark.sql to pull the tables that we need from the MassHealth silver data as well as the provider information.</font>
#
#

df_MassHealth_clms_prof = spark.sql('select distinct "tufts" as partner, "Medicaid" as lineOfBusiness,  cast(null as string) as sublineOfBusiness, \
cast(null as string) as commonID,  false as capitatedFlag, \
false as inNetworkFlag, \
case when servicing_prov_ID is null then data.NPI_SERVICING end as  servicing_prov_ID, data.CDE_PROV_TYPE_SERVICING as servicing_specialty, case when billing_prov_ID is null then data.NPI_BILLING end as  billing_prov_ID,  data.CDE_PROV_TYPE_BILLING as billing_specialty, case when referring_prov_ID is null then data.NPI_REFERRING end as referring_prov_ID,  data.CDE_PROV_TYPE_REFERRING as referring_specialty, \
cast(null as float)  as cob, false as cobFlag, \
data.NUM_LOGICAL_CLAIM as partnerClaimID, "professional" as claimType, cast(data.NUM_DTL as INT) as lineNumber, \
"tufts-data" as project, "silver_claims" as dataset,"MassHealth" as table, \
data.CDE_DIAG_1, data.CDE_DIAG_2, data.CDE_DIAG_3, data.CDE_DIAG_4, data.CDE_DIAG_5, data.CDE_DIAG_6, data.CDE_DIAG_7, data.CDE_DIAG_8, data.CDE_DIAG_9, data.CDE_DIAG_10, data.CDE_DIAG_11, data.CDE_DIAG_12, data.CDE_DIAG_13, data.CDE_DIAG_14, data.CDE_DIAG_15, data.CDE_DIAG_16,data.CDE_DIAG_17, data.CDE_DIAG_18, data.CDE_DIAG_19, data.CDE_DIAG_20, data.CDE_DIAG_21, data.CDE_DIAG_22, data.CDE_DIAG_23, data.CDE_DIAG_24, data.CDE_DIAG_25, data.CDE_DIAG_26, \
identifier.surrogateId, pat.patientId, data.Mem_Id as partnerMemberID , \
concat(patient.patientId,data.NUM_LOGICAL_CLAIM) as claimID, \
data.DOS_FROM_DT as from, data.DOS_TO_DT as to, data.REMIT_DT as paid, \
data.CDE_PROC , data.CDE_PROC_MOD, data.CDE_PROC_MOD_2, data.CDE_PROC_MOD_3, data.CDE_PROC_MOD_4, \
data.AMT_BILLED as billed , cast(null as float) as coinsurance , data.AMT_PAID as planPaid, cast(null as float)  as copay, data.AMT_PAID_MCARE + data.AMT_PAID as allowed,  cast(null as string) as deductible ,  cast(data.QTY_UNITS_BILLED as INT) as serviceQuantity, \
case when data.CLAIM_STATUS = "P" then "Paid" when data.CLAIM_STATUS = "D" then "Denied" end as claimLineStatus , \
data.CDE_PLACE_OF_SERVICE as placeOfService , cast(null as string) as service_code, cast(null as float) as service_tier \
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
where (rev is null and bill is null ) ')

# <font size="3"> __Checkpoint 1__</font>
#
# <font size="3"> Here, and in other locations in the code, we institute a series of checkpoints to make the code run faster since it is a large amount of data.</font>
#

df_MassHealth_clms_prof.write.mode('overwrite').orc('gs://cbh-rachel-hong-spark-temp/checkpoints1/')

df_MassHealth_clms_prof = spark.read.orc('gs://cbh-rachel-hong-spark-temp/checkpoints1/')


# <font size="3">__Begin Transformation__</font>
#
# <font size="3"> After the checkpoint, we start the actual transformation of the files. The first step was to create a flatfile out of the existing data to simplify the arrays </font>
#

def proc_mods(df):
    cols = [col for col in df.columns if col.startswith("CDE_PROC_MOD")]
    kvs = explode(array([col(c).alias("proccode") for c in cols]))
    return df.withColumn("modifier", kvs) \
        .drop(*cols) 


# <font size="3">__Prepare Modifier Array__</font>
#
# <font size="3"> First, we need collect an array or modifiers at the line level </font>
#

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

# <font size="3">__Choose the data associated from the first line for the Header__</font>
#
# <font size="3"> We prepare use the surrogate ID and amounts associated with the first line of the claim, as in past claims </font>
#

justone = max_prep.select("claimID","surrogateId", "lineNumber", "project","dataset","table","from", "to","paid","CDE_PROC").where(max_prep.lineNumber == 1).withColumnRenamed("surrogateId", "maxsurrogateid").withColumnRenamed("project", "maxproject").withColumnRenamed("dataset", "maxdataset").withColumnRenamed("table", "maxtable").withColumnRenamed("discharge", "maxdischarge").withColumnRenamed("paid", "maxpaid").withColumnRenamed("from", "maxfrom").withColumnRenamed("to", "maxto").withColumnRenamed("admit", "maxadmit").withColumnRenamed("CDE_PROC", "maxCDE_PROC").dropDuplicates()
struct_prep = max_prep.join(justone,  on= ["claimID","lineNumber"], how='left').dropDuplicates().drop("modifier")


##Create dataFrame
df_MassHealth_clms_prof_structs = struct_prep \
    .withColumn("surrogate", struct( col("surrogateId").alias("id"),col("project"), col("dataset"),col("table"))) \
    .drop("surrogate_id","table","project","dataset") \
    .dropDuplicates()\
    .withColumn("date", struct(col("from"), col("to"), col("paid"))) \
    .withColumn("referring", struct(col("referring_prov_ID").alias("id"),col("referring_specialty").alias("specialty"))) \
    .withColumn("billing", struct(col("billing_prov_ID").alias("id"),col("billing_specialty").alias("specialty"))) \
    .drop("fromDate","toDate","paidDate") \
    .drop("billing_prov_ID","billing_specialty","referring_prov_ID","referring_specialty") \
    .withColumn("provider", struct(col("billing"), col("referring"))) \
    .drop("billing","referring") \
    .withColumn("servicing", struct(col("servicing_prov_ID").alias("id"),col("servicing_specialty").alias("specialty"))) \
    .drop("servicing_prov_ID","servicing_specialty") \
    .withColumn("servicing_provider", struct(col("servicing"))) \
    .drop("servicing") \
    .withColumn("amount", struct(col("allowed").cast(DecimalType(38,9)).alias("allowed"), col("billed").cast(DecimalType(38,9)).alias("billed"), col("cob").cast(DecimalType(38,9)).alias("cob"), col("copay").cast(DecimalType(38,9)).alias("copay"),col("deductible").cast(DecimalType(38,9)).alias("deductible"),col("coinsurance").cast(DecimalType(38,9)).alias("coinsurance"),col("planPaid").cast(DecimalType(38,9)).alias("planPaid"))) \
    .drop("allowed","billed","cob","copay","deductible","coinsurance","planPaid") \
    .withColumn("nullsurrogate", struct( lit(None).cast(StringType()).alias("id"), lit(None).cast(StringType()).alias("project"), lit(None).cast(StringType()).alias("dataset"),lit(None).cast(StringType()).alias("table"))) \
    .dropDuplicates()\
    .withColumn("maxsurrogate", struct( col("maxsurrogateid").alias("id"),col("maxproject").alias("project"), col("maxdataset").alias("dataset"),col("maxtable").alias("table"))) \
    .dropDuplicates()\
    .drop("maxsurrogateid","maxtable","maxproject","maxdataset") \
    .withColumn("memberIdentifier", struct( col("commonId"),col("partnerMemberId"),col("patientId"),col("partner"))) \
    .withColumn("procedure", 
                struct(
                    col("surrogate") , lit("primary").alias("tier"), lit("CPT").alias("codeset"), col("CDE_PROC").alias("code"),  col("modifiers")
                )
               ).dropDuplicates().drop("modifiers").dropDuplicates()


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
        .withColumn("diagnosis", struct(col("maxsurrogate").alias('surrogate'), col("diag.tier").alias("tier"), lit("ICD10Cm").alias('codeset'), col("diag.code").alias("code"))) \
        .drop(*cols) \
        .drop("diag")


def nullify(df):
    cols = [col for col in df.columns if col.startswith("CDE_DIAG")]
    kvs = explode(array([
      struct(
          col(c).alias("code"), 
          when(lit(c) == "CDE_DIAG_1", lit(None)).otherwise(lit(None)).alias("tier")
      )
        for c in cols
    ]))
    return df.withColumn("diag", kvs) \
        .withColumn("nulldiagnosis", struct(col("nullsurrogate").alias("surrogate"), col("diag.tier").cast(StringType()).alias("tier"), lit(None).cast(StringType()).alias('codeset'), lit(None).cast(StringType()).alias("code"))) \
        .drop(*cols) \
        .drop("diag")


nulldx = nullify(df_MassHealth_clms_prof_structs).dropDuplicates().select("claimID","nulldiagnosis")
nulldxs = nulldx.groupby("claimID").agg(F.collect_set("nulldiagnosis").alias("nulldiagnoses")).dropDuplicates()


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


newDf = collapse_cdes(df_MassHealth_clms_prof_structs).dropDuplicates()

newDfpre1 = collapse_cdes(df_MassHealth_clms_prof_structs2).dropDuplicates()
newDfpre = newDfpre1.where(newDfpre1.diagnosis.code.isNotNull()).where(newDfpre1.diagnosis.surrogate.id.isNotNull())
diagnoses = newDfpre.groupby("claimID").agg(F.collect_set("diagnosis").alias("diagnoses")).dropDuplicates()


filtered = newDfpre1.drop("diagnoses").dropDuplicates().drop("diagnosis").dropDuplicates()


# <font size="3">__Checkpoint 2__</font>
#
# <font size="3"> Another checkpoint for ease memory issues and dedupe</font>

filtered.write.mode('overwrite').orc('gs://cbh-rachel-hong-spark-temp/checkpoints2/')

orig_df = spark.read.orc('gs://cbh-rachel-hong-spark-temp/checkpoints2/').dropDuplicates().drop("amount")

# <font size="3">__Create array of service codes__</font>
#
# <font size="3"> Another checkpoint for ease memory issues</font>

services = orig_df.groupBy("claimID", "lineNumber").agg(F.collect_set(
    struct(        
        col("service_tier").cast(IntegerType()).alias("tier"),
        col("service_code").alias("code")
    )
).alias("typesOfService"))

lines_prep = orig_df.join(services, ["lineNumber","claimID"], how ='left').dropDuplicates().join(nulldxs, "claimID", how ='left').dropDuplicates().join(amounts, ["lineNumber","claimID"]).dropDuplicates()


lines_df = lines_prep.groupBy("claimID").agg(F.collect_set(
    struct(
        col("surrogate"),
        col("lineNumber"),
        col("cobFlag"),
        col("capitatedFlag"),
        col("claimLineStatus"),
        col("inNetworkFlag"),
        col("serviceQuantity"),
        col("placeOfService"),
        col("date"),
        col("servicing_provider").alias("provider"),
        col("procedure"),
        col("amount"),
        col("nulldiagnoses").alias("diagnoses"),
        col("typesOfService")
    )
).alias("lines")).dropDuplicates()

# <font size="3">__Checkpoint 3__</font>
#
# <font size="3"> Another checkpoint for ease memory issues</font>

lines_df.write.mode('overwrite').orc('gs://cbh-rachel-hong-spark-temp/checkpoints_3/')

fresh_lines_df = spark.read.orc('gs://cbh-rachel-hong-spark-temp/checkpoints_3/')

new_orig = orig_df.select("memberIdentifier", "claimID","partnerClaimID", "lineOfBusiness","sublineOfBusiness","provider"
        ).where(orig_df.lineNumber == 1).join(fresh_lines_df, "claimID", how ="inner").join(diagnoses, "claimID", how ="left") 

# <font size="3">__Assemble the final Dataframe__</font>
#
# <font size="3"> In preparation of the final load into Big Query, the final dateframe is assembled.</font>

final_df = new_orig \
    .select(
        "claimID", 
        "memberIdentifier", 
        struct(
            col("partnerClaimID"),
            col("lineOfBusiness"),
            col("sublineOfBusiness"),
            col("provider"),
            col("diagnoses")
        ).alias("header"),
        "lines"
    ).dropDuplicates()

# <font size="3">__Write the Information to a Table__</font>
#
# <font size="3"> The last step is to write the data to a table. Below, the data is being written to a sandbox, but could be written directly to silver of gold with the correct permissions.</font>

final_df.write.mode('overwrite').format('bigquery').options(
    **{
        'table':'tufts-data.gold_claims_incremental.MassHealth_Professional_20210310',
        'intermediateFormat':'orc',
        'mode':'overwrite'
    }
).save()




