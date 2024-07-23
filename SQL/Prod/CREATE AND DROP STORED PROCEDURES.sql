USE [MDMDW]
GO
/****** Object:  StoredProcedure [MDM].[usp_PERFORM_AGGREGATIONS_ATTRIBUE_BASED]    Script Date: 7/11/2024 11:59:19 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [MDM].[usp_PERFORM_AGGREGATIONS_ATTRIBUE_BASED]
WITH EXEC AS CALLER
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	--exec [MDM].[usp_PERFORM_AGGREGATIONS_ATTRIBUE_BASED]

	-- Daily aggregate data.
	DECLARE @AggregateStartDateTime DATETIME;
	SET @AggregateStartDateTime = CONVERT (DATETIME, DATEDIFF(DAY, 2, GETDATE()));

	DECLARE @AggregateEndDateTime DATETIME;
	SET @AggregateEndDateTime = CONVERT (DATETIME, DATEDIFF(DAY, 1, GETDATE()));

	-- Cleanup data that may be present in the time period we are creating new data for.
	DELETE [MDM].[F_INTERVAL_READ_BY_ATTRIBUTE] FROM [MDM].[F_INTERVAL_READ_BY_ATTRIBUTE] a 
	WHERE INTERVAL_END_DT_TM > @AggregateStartDateTime AND INTERVAL_END_DT_TM <= @AggregateEndDateTime
	
	-- Aggregate Substation Transformer data 
	INSERT INTO MDM.F_INTERVAL_READ_BY_ATTRIBUTE ([INTERVAL_END_DT_TM], [UOM_NBR], [AGGREGATE_ATTRIBUTE_ID], [VALUE], [METER_CNT])
	SELECT * FROM 
		(SELECT ir.INTERVAL_END_DT_TM,
			uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, 
			ROUND(SUM(ir.value),4) AS value, 
			count(*) AS meter_cnt
		FROM NISC.bi_srv_loc_hist_installed_meters_view m INNER JOIN [NISC].[D_FEEDER] f ON m.bi_fdr = F.FEEDER_NBR
			INNER JOIN [NISC].[D_SUBSTATION_TRANSFORMER] st ON st.SUBSTATION_TRANSFORMER_NBR = f.SUBSTATION_TRANSFORMER_NBR
			INNER JOIN [MDM].[D_AGGREGATE_ATTRIBUTE] aa ON st.SUBSTATION_TRANSFORMER_NBR = aa.AGGREGATE_ATTRIBUTE_VALUE
			INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
			INNER JOIN mdm.DIM_UOM uom ON ir.UOM_NBR = uom.UOM_NBR
		WHERE 
			aa.AGGREGATE_ATTRIBUTE_TYPE = 'Substation Transformer' --'101T1'
		AND m.bi_fdr = F.FEEDER_NBR
		AND m.bi_sub = st.SUBSTATION_NBR
		AND (uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 1) OR uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2))
		-- Debug
		--AND ir.INTERVAL_END_DT_TM >= '2024-07-13 00:00:00.000'
		--AND ir.INTERVAL_END_DT_TM <= '2024-07-14 00:00:00.000'
		AND ir.INTERVAL_END_DT_TM > @AggregateStartDateTime AND ir.INTERVAL_END_DT_TM <= @AggregateEndDateTime
		GROUP BY uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, aa.AGGREGATE_ATTRIBUTE_VALUE, ir.INTERVAL_END_DT_TM) AS Result
		ORDER BY INTERVAL_END_DT_TM, AGGREGATE_ATTRIBUTE_ID, UOM_NBR
	
	-- Aggregate Feeder data 
	INSERT INTO MDM.F_INTERVAL_READ_BY_ATTRIBUTE ([INTERVAL_END_DT_TM], [UOM_NBR], [AGGREGATE_ATTRIBUTE_ID], [VALUE], [METER_CNT])
	-- Non-tuned version
	--SELECT * FROM 
	--	(SELECT	ir.INTERVAL_END_DT_TM,
	--		uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, 
	--		ROUND(SUM(ir.value),4) AS value, 
	--		count(*) AS meter_cnt
	--	FROM NISC.bi_srv_loc_hist_installed_meters_view m INNER JOIN [NISC].[D_FEEDER] f ON m.bi_fdr = F.FEEDER_NBR
	--		INNER JOIN [NISC].[D_SUBSTATION_TRANSFORMER] st ON st.SUBSTATION_TRANSFORMER_NBR = f.SUBSTATION_TRANSFORMER_NBR
	--		INNER JOIN [MDM].[D_AGGREGATE_ATTRIBUTE] aa ON f.FEEDER_NAME = aa.AGGREGATE_ATTRIBUTE_VALUE
	--		INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
	--		INNER JOIN mdm.DIM_UOM uom ON ir.UOM_NBR = uom.UOM_NBR
	--	WHERE 
	--		aa.AGGREGATE_ATTRIBUTE_TYPE = 'Feeder'
	--	AND m.bi_fdr = F.FEEDER_NBR
	--	AND m.bi_sub = st.SUBSTATION_NBR
	--	AND (uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 1) OR uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2))
	--	-- !!!! Fix to go one day back with actual data !!!!
	--	AND ir.INTERVAL_END_DT_TM > '2024-05-15 00:00:00.000'
	--	AND ir.INTERVAL_END_DT_TM <= '2024-05-16 00:00:00.000'
	--	GROUP BY uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, aa.AGGREGATE_ATTRIBUTE_VALUE, ir.INTERVAL_END_DT_TM) AS Result
	--	ORDER BY INTERVAL_END_DT_TM, AGGREGATE_ATTRIBUTE_ID, UOM_NBR
	-- **** DB Tuner Version ****
	SELECT * 
	  FROM (SELECT ir.INTERVAL_END_DT_TM, 
				   uom.UOM_NBR, 
				   aa.AGGREGATE_ATTRIBUTE_ID, 
				   ROUND(SUM(ir.value), 4) AS value, 
				   count(*) AS meter_cnt 
			  FROM NISC.bi_srv_loc_hist_installed_meters_view m 
				   INNER JOIN [NISC].[D_FEEDER] f 
					  ON m.bi_fdr = F.FEEDER_NBR 
						 AND m.bi_fdr = F.FEEDER_NBR 
				   INNER JOIN [NISC].[D_SUBSTATION_TRANSFORMER] st 
					  ON m.bi_sub = st.SUBSTATION_NBR 
						 AND st.SUBSTATION_TRANSFORMER_NBR = f.SUBSTATION_TRANSFORMER_NBR 
				   INNER JOIN [MDM].[D_AGGREGATE_ATTRIBUTE] aa 
					  ON f.FEEDER_NAME = aa.AGGREGATE_ATTRIBUTE_VALUE 
				   INNER JOIN mdm.interval_read_view ir 
					  ON m.bi_mtr_nbr = ir.bi_mtr_nbr 
				   INNER JOIN mdm.DIM_UOM uom 
					  ON ir.UOM_NBR = uom.UOM_NBR 
			 WHERE aa.AGGREGATE_ATTRIBUTE_TYPE = 'Feeder' 
			   AND (uom.UOM_NBR = (SELECT uom_nbr 
									 FROM mdm.dim_uom_view T1CORE_DW_MDM_DIM_UOM_VIEW1 
									WHERE uom = 'KWH_USAGE' 
									  AND channel = 1) 
					 OR uom.UOM_NBR = (SELECT uom_nbr 
										 FROM mdm.dim_uom_view T1CORE_DW_MDM_DIM_UOM_VIEW2 
										WHERE uom = 'KWH_USAGE' 
										  AND channel = 2)) 
			   -- Debug
			   --AND ir.INTERVAL_END_DT_TM > '2024-05-15 00:00:00.000' 
			   --AND ir.INTERVAL_END_DT_TM <= (SELECT '2024-05-16 00:00:00.000'
			   AND ir.INTERVAL_END_DT_TM > @AggregateStartDateTime AND ir.INTERVAL_END_DT_TM <= @AggregateEndDateTime
			 GROUP BY uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, aa.AGGREGATE_ATTRIBUTE_VALUE, ir.INTERVAL_END_DT_TM) AS Result 
	 ORDER BY INTERVAL_END_DT_TM, AGGREGATE_ATTRIBUTE_ID, UOM_NBR

	-- Aggregate Feeder data by Phase
	INSERT INTO MDM.F_INTERVAL_READ_BY_ATTRIBUTE ([INTERVAL_END_DT_TM], [UOM_NBR], [AGGREGATE_ATTRIBUTE_ID], [VALUE], [METER_CNT])
	SELECT * FROM 
		(SELECT	ir.INTERVAL_END_DT_TM,
			uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, 
			ROUND(SUM(ir.value),4) AS value, 
			count(*) AS meter_cnt
		FROM NISC.bi_srv_loc_hist_installed_meters_view m INNER JOIN [NISC].[D_FEEDER] f ON m.bi_fdr = F.FEEDER_NBR
			INNER JOIN [NISC].[D_SUBSTATION_TRANSFORMER] st ON st.SUBSTATION_TRANSFORMER_NBR = f.SUBSTATION_TRANSFORMER_NBR
			INNER JOIN [MDM].[D_AGGREGATE_ATTRIBUTE] aa ON f.FEEDER_NAME = SUBSTRING(aa.AGGREGATE_ATTRIBUTE_VALUE,1,5)
			INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
			INNER JOIN mdm.DIM_UOM uom ON ir.UOM_NBR = uom.UOM_NBR
		WHERE 
			aa.AGGREGATE_ATTRIBUTE_TYPE LIKE 'Feeder by%'
		AND m.bi_fdr = F.FEEDER_NBR
		AND m.bi_sub = st.SUBSTATION_NBR
		AND m.bi_pri_phs = aa.AGGREGATE_ATTRIBUTE_PHASE
		AND (uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 1) OR uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2))
		-- Debug
		--AND ir.INTERVAL_END_DT_TM > '2024-05-15 00:00:00.000'
		--AND ir.INTERVAL_END_DT_TM <= '2024-05-16 00:00:00.000'
		AND ir.INTERVAL_END_DT_TM > @AggregateStartDateTime AND ir.INTERVAL_END_DT_TM <= @AggregateEndDateTime
		GROUP BY uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, aa.AGGREGATE_ATTRIBUTE_VALUE, ir.INTERVAL_END_DT_TM) AS Result
		ORDER BY INTERVAL_END_DT_TM, AGGREGATE_ATTRIBUTE_ID, UOM_NBR

	-- Aggregate Distribution Transformer data 
	INSERT INTO MDM.F_INTERVAL_READ_BY_ATTRIBUTE ([INTERVAL_END_DT_TM], [UOM_NBR], [AGGREGATE_ATTRIBUTE_ID], [VALUE], [METER_CNT])
	SELECT * FROM 
		(SELECT	ir.INTERVAL_END_DT_TM,
			uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, 
			ROUND(SUM(ir.value),4) AS value, 
			count(*) AS meter_cnt
		FROM NISC.bi_srv_loc_hist_installed_meters_view m 
			INNER JOIN [MDM].[D_AGGREGATE_ATTRIBUTE] aa ON m.BI_TRF_NBR = aa.AGGREGATE_ATTRIBUTE_VALUE
			INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
			INNER JOIN mdm.DIM_UOM uom ON ir.UOM_NBR = uom.UOM_NBR
		WHERE 
			aa.AGGREGATE_ATTRIBUTE_TYPE = 'Distribution Transformer'
		AND (uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 1) OR uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2))
		-- Debug
		--AND ir.INTERVAL_END_DT_TM > '2024-05-15 00:00:00.000'
		--AND ir.INTERVAL_END_DT_TM <= '2024-05-16 00:00:00.000'
		AND ir.INTERVAL_END_DT_TM > @AggregateStartDateTime AND ir.INTERVAL_END_DT_TM <= @AggregateEndDateTime
		GROUP BY uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, aa.AGGREGATE_ATTRIBUTE_VALUE, ir.INTERVAL_END_DT_TM) AS Result
		ORDER BY INTERVAL_END_DT_TM, AGGREGATE_ATTRIBUTE_ID, UOM_NBR


	-- Aggregate Distribution Transformer data by Phase
	INSERT INTO MDM.F_INTERVAL_READ_BY_ATTRIBUTE ([INTERVAL_END_DT_TM], [UOM_NBR], [AGGREGATE_ATTRIBUTE_ID], [VALUE], [METER_CNT])
	SELECT * FROM (SELECT	ir.INTERVAL_END_DT_TM,
			uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, 
			ROUND(SUM(ir.value),4) AS value, 
			count(*) AS meter_cnt
		FROM NISC.bi_srv_loc_hist_installed_meters_view m 
			INNER JOIN [MDM].[D_AGGREGATE_ATTRIBUTE] aa ON m.BI_TRF_NBR = aa.AGGREGATE_ATTRIBUTE_VALUE
			INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
			INNER JOIN mdm.DIM_UOM uom ON ir.UOM_NBR = uom.UOM_NBR
		WHERE 
			aa.AGGREGATE_ATTRIBUTE_TYPE LIKE 'Distribution Transformer by%'
		AND m.bi_pri_phs = aa.AGGREGATE_ATTRIBUTE_PHASE
		AND (uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 1) OR uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2))
		-- Debug
		--AND ir.INTERVAL_END_DT_TM > '2024-05-15 00:00:00.000'
		--AND ir.INTERVAL_END_DT_TM <= '2024-05-16 00:00:00.000'
		AND ir.INTERVAL_END_DT_TM > @AggregateStartDateTime AND ir.INTERVAL_END_DT_TM <= @AggregateEndDateTime
		GROUP BY uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, aa.AGGREGATE_ATTRIBUTE_VALUE, ir.INTERVAL_END_DT_TM) AS Result
		ORDER BY INTERVAL_END_DT_TM, AGGREGATE_ATTRIBUTE_ID, UOM_NBR


	-- Aggregate data by Rate
	INSERT INTO MDM.F_INTERVAL_READ_BY_ATTRIBUTE ([INTERVAL_END_DT_TM], [UOM_NBR], [AGGREGATE_ATTRIBUTE_ID], [VALUE], [METER_CNT])
	SELECT * FROM 
		(SELECT	ir.INTERVAL_END_DT_TM,
			uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, 
			ROUND(SUM(ir.value),4) AS value, 
			count(*) AS meter_cnt
		FROM NISC.bi_srv_loc_hist_installed_meters_view m 
			INNER JOIN [MDM].[D_AGGREGATE_ATTRIBUTE] aa ON m.BI_RATE_SCHED = aa.AGGREGATE_ATTRIBUTE_VALUE
			INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
			INNER JOIN mdm.DIM_UOM uom ON ir.UOM_NBR = uom.UOM_NBR
		WHERE 
			aa.AGGREGATE_ATTRIBUTE_TYPE = 'Rate'
		AND (uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 1) OR uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2))
		-- Debug
		--AND ir.INTERVAL_END_DT_TM > '2024-05-15 00:00:00.000'
		--AND ir.INTERVAL_END_DT_TM <= '2024-05-16 00:00:00.000'
		AND ir.INTERVAL_END_DT_TM > @AggregateStartDateTime AND ir.INTERVAL_END_DT_TM <= @AggregateEndDateTime
		GROUP BY uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, aa.AGGREGATE_ATTRIBUTE_VALUE, ir.INTERVAL_END_DT_TM) AS Result
		ORDER BY INTERVAL_END_DT_TM, AGGREGATE_ATTRIBUTE_ID, UOM_NBR


	-- Aggregate data by Distributed Generation – All Production Meters
	INSERT INTO MDM.F_INTERVAL_READ_BY_ATTRIBUTE ([INTERVAL_END_DT_TM], [UOM_NBR], [AGGREGATE_ATTRIBUTE_ID], [VALUE], [METER_CNT])
	SELECT * FROM 
		(SELECT	ir.INTERVAL_END_DT_TM,
			uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, 
			ROUND(SUM(ir.value),4) AS value, 
			count(*) AS meter_cnt
		FROM NISC.bi_srv_loc_hist_installed_meters_view m 
			INNER JOIN [MDM].[D_AGGREGATE_ATTRIBUTE] aa ON aa.AGGREGATE_ATTRIBUTE_TYPE = 'DG – All Prod Meters'
			INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
			INNER JOIN mdm.DIM_UOM uom ON ir.UOM_NBR = uom.UOM_NBR
		WHERE 
		m.BI_MTR_POS_NBR = 2 AND m.BI_RATE_SCHED =  'PROD' 
		AND (uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 1) OR uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2))
		-- Debug
		--AND ir.INTERVAL_END_DT_TM > '2024-05-15 00:00:00.000'
		--AND ir.INTERVAL_END_DT_TM <= '2024-05-16 00:00:00.000'
		AND ir.INTERVAL_END_DT_TM > @AggregateStartDateTime AND ir.INTERVAL_END_DT_TM <= @AggregateEndDateTime
		AND m.bi_mtr_nbr NOT IN (SELECT BI_MTR_NBR FROM [NISC].[D_METER_FILTER] WHERE [AGGREGATE_ATTRIBUTE_TYPE] = 'DG – All Prod Meters')
		GROUP BY uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, aa.AGGREGATE_ATTRIBUTE_VALUE, ir.INTERVAL_END_DT_TM) AS Result
		ORDER BY INTERVAL_END_DT_TM, AGGREGATE_ATTRIBUTE_ID, UOM_NBR

	-- Aggregate data by Distributed Generation – All Net Meters
	INSERT INTO MDM.F_INTERVAL_READ_BY_ATTRIBUTE ([INTERVAL_END_DT_TM], [UOM_NBR], [AGGREGATE_ATTRIBUTE_ID], [VALUE], [METER_CNT])
	SELECT * FROM 
		(SELECT	ir.INTERVAL_END_DT_TM,
			uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, 
			ROUND(SUM(ir.value),4) AS value, 
			count(*) AS meter_cnt
		FROM NISC.bi_srv_loc_hist_installed_meters_view m 
			INNER JOIN [MDM].[D_AGGREGATE_ATTRIBUTE] aa ON aa.AGGREGATE_ATTRIBUTE_TYPE = 'DG – All Net Meters'
			INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
			INNER JOIN mdm.DIM_UOM uom ON ir.UOM_NBR = uom.UOM_NBR
		WHERE 
		m.BI_NET_METER_SW = 'Y'
		AND (uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 1) OR uom.UOM_NBR = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2))
		-- Debug
		--AND ir.INTERVAL_END_DT_TM > '2024-05-15 00:00:00.000'
		--AND ir.INTERVAL_END_DT_TM <= '2024-05-16 00:00:00.000'
		AND ir.INTERVAL_END_DT_TM > @AggregateStartDateTime AND ir.INTERVAL_END_DT_TM <= @AggregateEndDateTime
		GROUP BY uom.UOM_NBR, aa.AGGREGATE_ATTRIBUTE_ID, aa.AGGREGATE_ATTRIBUTE_VALUE, ir.INTERVAL_END_DT_TM) AS Result
		ORDER BY INTERVAL_END_DT_TM, AGGREGATE_ATTRIBUTE_ID, UOM_NBR


-- Test Data
--INSERT INTO [MDM].[D_AGGREGATE_ATTRIBUTE]
--           ([AGGREGATE_ATTRIBUTE_TYPE]
--           ,[AGGREGATE_ATTRIBUTE_VALUE])
--     VALUES
--           ('Substation Transformer',
--           '101T1')
--GO


--INSERT INTO [MDM].[D_AGGREGATE_ATTRIBUTE]
--           ([AGGREGATE_ATTRIBUTE_TYPE]
--           ,[AGGREGATE_ATTRIBUTE_VALUE])
--     VALUES
--           ('Feeder',
--           'F1011')
--GO

--INSERT INTO [MDM].[D_AGGREGATE_ATTRIBUTE]
--           ([AGGREGATE_ATTRIBUTE_TYPE]
--           ,[AGGREGATE_ATTRIBUTE_VALUE]
--		   ,[AGGREGATE_ATTRIBUTE_PHASE])
--     VALUES
--           ('Feeder by Phase (A)',
--           'F1011', 'A')

--INSERT INTO [MDM].[D_AGGREGATE_ATTRIBUTE]
--           ([AGGREGATE_ATTRIBUTE_TYPE]
--           ,[AGGREGATE_ATTRIBUTE_VALUE],[AGGREGATE_ATTRIBUTE_PHASE])
--     VALUES
--           ('Feeder by Phase (ABC)',
--           'F1011', 'ABC')
--GO

--INSERT INTO [MDM].[D_AGGREGATE_ATTRIBUTE]
--           ([AGGREGATE_ATTRIBUTE_TYPE]
--           ,[AGGREGATE_ATTRIBUTE_VALUE])
--     VALUES
--           ('Distribution Transformer',
--           'TU13661-1')
--GO

--INSERT INTO [MDM].[D_AGGREGATE_ATTRIBUTE]
--           ([AGGREGATE_ATTRIBUTE_TYPE]
--           ,[AGGREGATE_ATTRIBUTE_VALUE]
--           ,[AGGREGATE_ATTRIBUTE_PHASE])
--     VALUES
--           ('Distribution Transformer by Phase (B)',
--           'TU13661-1','B')
--GO

--INSERT INTO [MDM].[D_AGGREGATE_ATTRIBUTE]
--           ([AGGREGATE_ATTRIBUTE_TYPE]
--           ,[AGGREGATE_ATTRIBUTE_VALUE])
--     VALUES
--           ('Rate',
--           'A')
--GO

--INSERT INTO [MDM].[D_AGGREGATE_ATTRIBUTE]
--           ([AGGREGATE_ATTRIBUTE_TYPE]
--           ,[AGGREGATE_ATTRIBUTE_VALUE])
--     VALUES
--           ('DG – All Prod Meters',
--           'Y')
--GO

--INSERT INTO [MDM].[D_AGGREGATE_ATTRIBUTE]
--           ([AGGREGATE_ATTRIBUTE_TYPE]
--           ,[AGGREGATE_ATTRIBUTE_VALUE])
--     VALUES
--           ('DG – All Net Meters',
--           'Y')
--GO

--INSERT INTO [NISC].[D_METER_FILTER]
--([BI_MTR_NBR], [AGGREGATE_ATTRIBUTE_TYPE])
--VALUES ('1ND81119251','DG – All Prod Meters')

END

GO


CREATE PROCEDURE [MDM].[usp_PERFORM_AGGREGATIONS_DAILY] 
	-- Add the parameters for the stored procedure here

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	-- Daily aggregate data.
	DECLARE @AggregateEndDateTime DATETIME;
	SET @AggregateEndDateTime = CONVERT (DATETIME, DATEDIFF(DAY, 0, GETDATE()));

	DECLARE @Counter INT;
	SET @Counter = 0;
	DECLARE @AggregateUom INT;

	WHILE @Counter > -7
	BEGIN 
		SELECT @AggregateUom = uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 1
		EXEC [MDM].[usp_AGGREGATE_INTERVAL_READ_BY_DATE]  @AggregateEndDateTime, @AggregateUom
		SELECT @AggregateUom = uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2
		EXEC [MDM].[usp_AGGREGATE_INTERVAL_READ_BY_DATE] @AggregateEndDateTime, @AggregateUom
		SET @AggregateEndDateTime = DATEADD(DAY, -1, @AggregateEndDateTime)
		--print (@Counter)
		SET @Counter = @Counter -1;
		--print (@Counter)
	END

END

GO


CREATE PROCEDURE [MDM].[usp_PERFORM_AGGREGATIONS_SOLAR] 
	-- Add the parameters for the stored procedure here

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	-- Daily calculation of solar customer interval >10kw
	DELETE FROM MDM.INTERVAL_READ_EXCEEDING_10KW WHERE interval_end_dt_tm <= CONVERT (DATETIME, DATEDIFF(DAY, 45, GETDATE()))
	DELETE FROM MDM.INTERVAL_READ_EXCEEDING_10KW WHERE interval_end_dt_tm > CONVERT (DATETIME, DATEDIFF(DAY, 2, GETDATE()))

	INSERT INTO MDM.INTERVAL_READ_EXCEEDING_10KW (INTERVAL_END_DT_TM, BI_MTR_NBR, VALUE)
  SELECT Result.interval_end_dt_tm, Result.bi_mtr_nbr, Result.value FROM (
      SELECT dateadd(hour, datediff(hour, 0, DATEADD(MINUTE, -15, ir.interval_end_dt_tm)), 0) as interval_end_dt_tm, m.bi_mtr_nbr, sum(ir.value)/m.bi_ct_ratio as value
            FROM
                  mdm.bi_srv_loc_hist_installed_meters_view m INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
            WHERE
                  m.bi_net_meter_sw = 'Y'
                  AND ir.uom_nbr = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2)
                  AND ir.interval_end_dt_tm > CONVERT (DATETIME, DATEDIFF(DAY, 2, GETDATE()))
                  AND ((ir.value * 4)/m.bi_ct_ratio) > 10
                  --AND m.BI_MTR_NBR = '1ND84929775'
            GROUP BY m.bi_mtr_nbr, m.bi_ct_ratio, dateadd(hour, datediff(hour, 0, DATEADD(MINUTE, -15, ir.interval_end_dt_tm)), 0)
            ) As Result
      WHERE value > 10


END

GO

CREATE PROCEDURE [MDM].[usp_MERGE_SUBSTATION_TRANSFORMERS] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	-- Merge the Substations
	MERGE [NISC].[D_SUBSTATION] AS target
	USING
	(
		SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
			SUBSTATION_NAME,'T1(S)',''),'T2(N)',''),'T1(W)',''),'T2(E)',''),'T1(E)',''),'T1(N)',''),'T2(S)',''),'T2(W)',''),'T1(M)',''),'T3(S)',''),'T10',''),'T11',''),'T12',''),'T1',''),'T2',''),'T3',''),'T4',''),'T5',''),'T6',''),'T7',''),'T8',''),'T9','') AS SUBSTATION_NAME
				,SUBSTRING([SUBSTATION_TRANSFORMER_NBR],1,3) AS SUBSTATION_NBR 
				FROM [staging].[D_SUBSTATION_TRANSFORMER]
		GROUP BY REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
			SUBSTATION_NAME,'T1(S)',''),'T2(N)',''),'T1(W)',''),'T2(E)',''),'T1(E)',''),'T1(N)',''),'T2(S)',''),'T2(W)',''),'T1(M)',''),'T3(S)',''),'T10',''),'T11',''),'T12',''),'T1',''),'T2',''),'T3',''),'T4',''),'T5',''),'T6',''),'T7',''),'T8',''),'T9','')
				,SUBSTRING([SUBSTATION_TRANSFORMER_NBR],1,3)
	) AS source
	ON (target.SUBSTATION_NBR = source.SUBSTATION_NBR)
	--AND target.SUBSTATION_NAME = source.SUBSTATION_NAME)

	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([SUBSTATION_NBR], [SUBSTATION_NAME], [ACTIVE_YN], [INACTIVE_DATE_ID])
		VALUES (source.SUBSTATION_NBR, source.SUBSTATION_NAME, 1, NULL) 

	WHEN MATCHED THEN
		-- Update the value
		UPDATE SET 
				target.SUBSTATION_NAME = source.SUBSTATION_NAME

	WHEN NOT MATCHED BY SOURCE THEN
		-- Deactivate the record
		UPDATE SET 
				target.ACTIVE_YN = 0,
				target.INACTIVE_DATE_ID = (SELECT DATE_ID FROM CORE.D_DATE WHERE DATE = CONVERT(DATETIME, DATEDIFF(DAY, 0, GETDATE())))
		
	;

	-- Merge the Substation Transformers
	MERGE [NISC].[D_SUBSTATION_TRANSFORMER] AS target
	USING
	(
		SELECT SUBSTATION_TRANSFORMER_NBR
				,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
					SUBSTATION_NAME,'T1(S)',''),'T2(N)',''),'T1(W)',''),'T2(E)',''),'T1(E)',''),'T1(N)',''),'T2(S)',''),'T2(W)',''),'T1(M)',''),'T3(S)',''),'T10',''),'T11',''),'T12',''),'T1',''),'T2',''),'T3',''),'T4',''),'T5',''),'T6',''),'T7',''),'T8',''),'T9','') AS SUBSTATION_NAME
				,RIGHT([SUBSTATION_TRANSFORMER_NBR],2) AS SUBSTATION_TRANSFORMER
				,SUBSTRING([SUBSTATION_TRANSFORMER_NBR],1,3) AS SUBSTATION_NBR
				FROM [staging].[D_SUBSTATION_TRANSFORMER]
		GROUP BY SUBSTATION_TRANSFORMER_NBR,
				REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
					SUBSTATION_NAME,'T1(S)',''),'T2(N)',''),'T1(W)',''),'T2(E)',''),'T1(E)',''),'T1(N)',''),'T2(S)',''),'T2(W)',''),'T1(M)',''),'T3(S)',''),'T10',''),'T11',''),'T12',''),'T1',''),'T2',''),'T3',''),'T4',''),'T5',''),'T6',''),'T7',''),'T8',''),'T9','')
				,RIGHT([SUBSTATION_TRANSFORMER_NBR],2)
				,SUBSTRING([SUBSTATION_TRANSFORMER_NBR],1,3) 
	) AS source
	ON (target.SUBSTATION_TRANSFORMER_NBR = source.SUBSTATION_TRANSFORMER_NBR)

	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([SUBSTATION_TRANSFORMER_NBR], [SUBSTATION_TRANSFORMER], [SUBSTATION_NBR], [ACTIVE_YN], [INACTIVE_DATE_ID])
		VALUES (source.SUBSTATION_TRANSFORMER_NBR, source.SUBSTATION_TRANSFORMER, source.[SUBSTATION_NBR], 1, NULL) 

	WHEN NOT MATCHED BY SOURCE THEN
		-- Deactivate the record
		UPDATE SET 
				target.ACTIVE_YN = 0,
				target.INACTIVE_DATE_ID = (SELECT DATE_ID FROM CORE.D_DATE WHERE DATE = CONVERT(DATETIME, DATEDIFF(DAY, 0, GETDATE())))
		
	;
    
	-- Merge the Feeders
	MERGE [NISC].[D_FEEDER] AS target
	USING
	(
		SELECT (CASE WHEN LEN(FEEDER_NBR) = 5 THEN RIGHT(FEEDER_NBR,1) ELSE RIGHT(FEEDER_NBR,2) END) AS FEEDER_NBR
				,FEEDER_NBR AS FEEDER_NAME
				,SUBSTATION_TRANSFORMER_NBR
				FROM [staging].[D_SUBSTATION_TRANSFORMER]
	) AS source
	ON (target.FEEDER_NAME = source.FEEDER_NAME)

	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([FEEDER_NBR], [FEEDER_NAME], [SUBSTATION_TRANSFORMER_NBR], [ACTIVE_YN], [INACTIVE_DATE_ID])
		VALUES (source.FEEDER_NBR, source.FEEDER_NAME, source.SUBSTATION_TRANSFORMER_NBR, 1, NULL) 

	WHEN NOT MATCHED BY SOURCE THEN
		-- Deactivate the record
		UPDATE SET 
				target.ACTIVE_YN = 0,
				target.INACTIVE_DATE_ID = (SELECT DATE_ID FROM CORE.D_DATE WHERE DATE = CONVERT(DATETIME, DATEDIFF(DAY, 0, GETDATE())))
		
	;
END
GO


CREATE PROCEDURE [MDM].[usp_PERFORM_AGGREGATION_DATA_POPULATION]
WITH EXEC AS CALLER
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	--DELETE FROM [MDM].[F_INTERVAL_READ_BY_ATTRIBUTE]
	--DELETE FROM [MDM].[D_AGGREGATE_ATTRIBUTE]
	
	-- If there are new attributes merge them
	MERGE [MDM].[D_AGGREGATE_ATTRIBUTE] AS target
	USING
	(
		SELECT 
		   AGGREGATE_ATTRIBUTE_TYPE,
		   AGGREGATE_ATTRIBUTE_VALUE,
		   AGGREGATE_ATTRIBUTE_PHASE
	FROM   (SELECT 'Substation Transformer'   AS AGGREGATE_ATTRIBUTE_TYPE,
				   substation_transformer_nbr AS AGGREGATE_ATTRIBUTE_VALUE,
				   NULL                       AS AGGREGATE_ATTRIBUTE_PHASE
			FROM   [nisc].[d_substation_transformer]
			WHERE  active_yn = 1
			
		UNION ALL 
			
			SELECT 'Feeder'    AS AGGREGATE_ATTRIBUTE_TYPE,
				   feeder_name AS AGGREGATE_ATTRIBUTE_VALUE,
				   NULL        AS AGGREGATE_ATTRIBUTE_PHASE
			FROM   [nisc].[d_feeder]
			WHERE  active_yn = 1
		UNION ALL
		
			SELECT Concat('Feeder by Phase (', bi_pri_phs, ')') AS AGGREGATE_ATTRIBUTE_TYPE,
				   Concat('F', bi_sub, bi_fdr)                  AS AGGREGATE_ATTRIBUTE_VALUE,
				   bi_pri_phs                                   AS AGGREGATE_ATTRIBUTE_PHASE
			FROM   [NISC].[bi_srv_loc_hist_installed_meters_view]
			WHERE  bi_pri_phs IS NOT NULL
			GROUP  BY bi_sub,
					  bi_fdr,
					  bi_pri_phs
		UNION ALL 
		
			--SELECT 'Distribution Transformer' AS AGGREGATE_ATTRIBUTE_TYPE,
			--	   bi_trf_nbr                 AS AGGREGATE_ATTRIBUTE_VALUE,
			--	   NULL                       AS AGGREGATE_ATTRIBUTE_PHASE
			--FROM   [NISC].[bi_srv_loc_hist_installed_meters_view]
			--GROUP  BY bi_trf_nbr
			SELECT 'Distribution Transformer' AS AGGREGATE_ATTRIBUTE_TYPE,
				   sl.BI_TRF_NBR  AS AGGREGATE_ATTRIBUTE_VALUE,
				   NULL           AS AGGREGATE_ATTRIBUTE_PHASE
					FROM  [NISC].[BI_SRV_LOC_HIST_INSTALLED_METERS_VIEW] m 
						INNER JOIN [NISC].[BI_SRV_LINK] sl ON m.BI_SRV_LOC_NBR = sl.BI_SRV_LOC_NBR AND m.BI_MTR_POS_NBR = sl.BI_MTR_POS_NBR
					GROUP BY sl.BI_TRF_NBR
			
		UNION ALL 
			
			--SELECT Concat('Distribution Transformer by Phase (', bi_pri_phs, ')') AS AGGREGATE_ATTRIBUTE_TYPE,
			--	   bi_trf_nbr                                                     AS AGGREGATE_ATTRIBUTE_VALUE,
			--	   bi_pri_phs                                                     AS AGGREGATE_ATTRIBUTE_PHASE
			--FROM   [NISC].[bi_srv_loc_hist_installed_meters_view]
			--WHERE  bi_pri_phs IS NOT NULL
			--GROUP  BY bi_trf_nbr,
			--		  bi_pri_phs
			SELECT Concat('Distribution Transformer by Phase (', TRIM(sl.BI_MTR_SRV_PHS), ')')	AS AGGREGATE_ATTRIBUTE_TYPE,
				   sl.BI_TRF_NBR																AS AGGREGATE_ATTRIBUTE_VALUE,
				   TRIM(sl.BI_MTR_SRV_PHS)														AS AGGREGATE_ATTRIBUTE_PHASE
			FROM   [NISC].[BI_SRV_LOC_HIST_INSTALLED_METERS_VIEW] m 
						INNER JOIN [NISC].[BI_SRV_LINK] sl ON m.BI_SRV_LOC_NBR = sl.BI_SRV_LOC_NBR AND m.BI_MTR_POS_NBR = sl.BI_MTR_POS_NBR
			GROUP  BY sl.BI_TRF_NBR,
					  sl.BI_MTR_SRV_PHS

		UNION ALL
			
			SELECT 'Rate'        AS AGGREGATE_ATTRIBUTE_TYPE,
				   bi_rate_sched AS AGGREGATE_ATTRIBUTE_VALUE,
				   NULL          AS AGGREGATE_ATTRIBUTE_PHASE
			FROM   [NISC].[bi_srv_loc_hist_installed_meters_view]
			WHERE  bi_rate_sched IS NOT NULL
			GROUP  BY bi_rate_sched
		
		UNION ALL
		
			SELECT 'DG – All Prod Meters' AS AGGREGATE_ATTRIBUTE_TYPE,
				   'Y'                      AS AGGREGATE_ATTRIBUTE_VALUE,
				   NULL                     AS AGGREGATE_ATTRIBUTE_PHASE
			
		UNION ALL
		
			SELECT 'DG – All Net Meters' AS AGGREGATE_ATTRIBUTE_TYPE,
				   'Y'                     AS AGGREGATE_ATTRIBUTE_VALUE,
				   NULL                    AS AGGREGATE_ATTRIBUTE_PHASE) a 
	) AS source
	ON (target.[AGGREGATE_ATTRIBUTE_TYPE] = source.[AGGREGATE_ATTRIBUTE_TYPE]
			AND target.[AGGREGATE_ATTRIBUTE_VALUE] = source.[AGGREGATE_ATTRIBUTE_VALUE]
		)

	WHEN NOT MATCHED BY TARGET THEN

		INSERT (AGGREGATE_ATTRIBUTE_TYPE, AGGREGATE_ATTRIBUTE_VALUE, AGGREGATE_ATTRIBUTE_PHASE)
		VALUES (source.AGGREGATE_ATTRIBUTE_TYPE, source.AGGREGATE_ATTRIBUTE_VALUE, source.AGGREGATE_ATTRIBUTE_PHASE) 

	;
	

END
GO


