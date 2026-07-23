"""
Transform TBAADM_GAM_TDY & TBAADM_FBM_TDY -> AR_X_AU
Implements mapping logic for Blocks 1, 2, 3, 7 (GAM) and Block 8 (FBM).
Supports selective block execution via --blocks parameter (e.g. --blocks 7,8).
"""

import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, lit, to_date, when

sys.path.append("/opt/spark")

from jobs.common.constants import SOR_AR_X_AU, TABLE_FBM_TDY, TABLE_GAM_TDY
from jobs.config.job_config import JobConfig
from jobs.utils.cli_parser import parse_etl_args
from jobs.utils.hash_function import function_hash02
from jobs.utils.minio_writer import write_parquet
from jobs.utils.spark_session import build_spark


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    try:
        args = parse_etl_args()
        etl_date_str = str(args.etl_date)
        run_type_str = str(args.run_type).upper() if args.run_type else "FULL"
        blocks_arg = str(getattr(args, "blocks", "8")).lower().strip()

        if blocks_arg == "all":
            selected_blocks = {"1", "2", "3", "7", "8"}
        else:
            selected_blocks = {b.strip() for b in blocks_arg.split(",") if b.strip()}

        logger.info(
            f"Starting SOR transformation job for {SOR_AR_X_AU}. "
            f"ETL Date: {etl_date_str}, Run Type: {run_type_str}, "
            f"Executing Blocks: {sorted(list(selected_blocks))}"
        )

        spark = build_spark("sor_ar_x_au")
        config = JobConfig()
        raw_bucket = config.raw_bucket
        sor_bucket = config.sor_bucket

        # Shared date expressions
        etl_date_col = to_date(lit(etl_date_str), "yyyyMMdd")
        end_date_col = to_date(lit("99991231"), "yyyyMMdd")

        blocks_to_union = []

        # ──────────────────────────────────────────────────────────────
        # 1. GAM BLOCKS (1, 2, 3, 7)
        # ──────────────────────────────────────────────────────────────
        gam_blocks_needed = selected_blocks.intersection({"1", "2", "3", "7"})
        if gam_blocks_needed:
            gam_input_path = f"s3a://{raw_bucket}/{TABLE_GAM_TDY}/"
            logger.info(f"Reading raw GAM data from {gam_input_path} for blocks {sorted(list(gam_blocks_needed))}")
            df_raw_gam = spark.read.parquet(gam_input_path)

            gam_filtered = df_raw_gam.filter(
                (col("ENTITY_CRE_FLG") == "Y") & (col("DEL_FLG") == "N")
            )

            if "BANK_ID" in df_raw_gam.columns:
                gam_filtered = gam_filtered.filter(col("BANK_ID") == "01")

            eff_date_gam_col = when(
                lit(run_type_str).isin("FIRST", "FULL"),
                to_date(col("ACCT_OPN_DATE").cast("string"))
            ).otherwise(etl_date_col)

            # ── Block 1 ──
            if "1" in selected_blocks:
                logger.info("Processing Block 1 (AR_X_GLSH)")
                b1 = gam_filtered.select(
                    function_hash02("AR|TBAADM.GAM", col("ACID")).alias("AR_ID"),
                    function_hash02("CL|SRC_STM", lit("TBAADM.GAM")).alias("SRC_STM_ID"),
                    function_hash02(
                        "AU|TBAADM.GSH",
                        concat(col("GL_SUB_HEAD_CODE"), lit("."), col("SOL_ID"), lit("."), col("ACCT_CRNCY_CODE"))
                    ).alias("AU_ID"),
                    function_hash02("CL|AR_X_AU_TP", lit("AR_X_GLSH")).alias("AR_X_AU_RLTNP_TP_ID"),
                    etl_date_col.alias("PPN_DT"),
                    eff_date_gam_col.alias("EFF_DT"),
                    end_date_col.alias("END_DT"),
                )
                blocks_to_union.append(b1)

            # ── Block 2 ──
            if "2" in selected_blocks:
                logger.info("Processing Block 2 (AR_X_ACT_CLS_BAL)")
                b2 = gam_filtered.select(
                    function_hash02("AR|TBAADM.GAM", col("ACID")).alias("AR_ID"),
                    function_hash02("CL|SRC_STM", lit("TBAADM.GAM")).alias("SRC_STM_ID"),
                    function_hash02("AU|TBAADM.GAM", concat(col("ACID"), lit(".CLS_BAL"))).alias("AU_ID"),
                    function_hash02("CL|AR_X_AU_TP", lit("AR_X_ACT_CLS_BAL")).alias("AR_X_AU_RLTNP_TP_ID"),
                    etl_date_col.alias("PPN_DT"),
                    eff_date_gam_col.alias("EFF_DT"),
                    end_date_col.alias("END_DT"),
                )
                blocks_to_union.append(b2)

            # ── Block 3 ──
            if "3" in selected_blocks:
                logger.info("Processing Block 3 (AR_X_ACT_USED_PRVN_AMT)")
                b3 = gam_filtered.filter(
                    col("SCHM_TYPE").isin("CLA", "LAA", "ODA")
                ).select(
                    function_hash02("AR|TBAADM.GAM", col("ACID")).alias("AR_ID"),
                    function_hash02("CL|SRC_STM", lit("TBAADM.GAM")).alias("SRC_STM_ID"),
                    function_hash02("AU|TBAADM.GAM", concat(col("ACID"), lit(".USED_PRVN_AMT"))).alias("AU_ID"),
                    function_hash02("CL|AR_X_AU_TP", lit("AR_X_ACT_USED_PRVN_AMT")).alias("AR_X_AU_RLTNP_TP_ID"),
                    etl_date_col.alias("PPN_DT"),
                    eff_date_gam_col.alias("EFF_DT"),
                    end_date_col.alias("END_DT"),
                )
                blocks_to_union.append(b3)

            # ── Block 7 ──
            if "7" in selected_blocks:
                logger.info("Processing Block 7 (AR_X_ACT_BAL - GAM)")
                b7 = gam_filtered.select(
                    function_hash02("AR|TBAADM.GAM", col("ACID")).alias("AR_ID"),
                    function_hash02("CL|SRC_STM", lit("TBAADM.GAM")).alias("SRC_STM_ID"),
                    function_hash02("AU|TBAADM.GAM", concat(col("ACID"), lit(".BAL"))).alias("AU_ID"),
                    function_hash02("CL|AR_X_AU_TP", lit("AR_X_ACT_BAL")).alias("AR_X_AU_RLTNP_TP_ID"),
                    etl_date_col.alias("PPN_DT"),
                    eff_date_gam_col.alias("EFF_DT"),
                    end_date_col.alias("END_DT"),
                )
                blocks_to_union.append(b7)

        # ──────────────────────────────────────────────────────────────
        # 2. FBM BLOCK (8)
        # ──────────────────────────────────────────────────────────────
        if "8" in selected_blocks:
            fbm_input_path = f"s3a://{raw_bucket}/{TABLE_FBM_TDY}/"
            logger.info(f"Reading raw FBM data from {fbm_input_path} for Block 8")
            df_raw_fbm = spark.read.parquet(fbm_input_path)

            fbm_filtered = df_raw_fbm.filter(
                (col("DEL_FLG") == "N") & (col("ENTITY_CRE_FLG") == "Y")
            )

            logger.info("Processing Block 8 (AR_X_ACT_BAL - FBM)")
            b8 = fbm_filtered.select(
                function_hash02("AR|TBAADM.FBM", col("BILL_ID")).alias("AR_ID"),
                function_hash02("CL|SRC_STM", lit("TBAADM.FBM")).alias("SRC_STM_ID"),
                function_hash02("AU|TBAADM.FBM", concat(col("BILL_ID"), lit(".BAL"))).alias("AU_ID"),
                function_hash02("CL|AR_X_AU_TP", lit("AR_X_ACT_BAL")).alias("AR_X_AU_RLTNP_TP_ID"),
                etl_date_col.alias("PPN_DT"),
                etl_date_col.alias("EFF_DT"),
                end_date_col.alias("END_DT"),
            )
            blocks_to_union.append(b8)

        if not blocks_to_union:
            raise ValueError(f"No valid blocks selected to run! Given: {blocks_arg}")

        # ──────────────────────────────────────────────────────────────
        # 3. UNION & WRITE OUTPUT
        # ──────────────────────────────────────────────────────────────
        logger.info(f"Unioning {len(blocks_to_union)} selected block(s)...")
        df_final = blocks_to_union[0]
        for df_b in blocks_to_union[1:]:
            df_final = df_final.union(df_b)

        output_path = f"{SOR_AR_X_AU}/"
        logger.info(f"Writing transformed data to s3a://{sor_bucket}/{output_path}")
        write_parquet(df_final, sor_bucket, output_path, mode="overwrite")

        logger.info(f"Successfully completed SOR transformation job for {SOR_AR_X_AU}")

    except Exception as e:
        logger.error(f"SOR transformation job failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
