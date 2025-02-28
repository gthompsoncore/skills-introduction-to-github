USE [MDMDW]
GO
/****** Object:  StoredProcedure [MDM].[usp_PERFORM_AGGREGATIONS]    Script Date: 7/22/2024 9:03:54 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [MDM].[usp_PERFORM_AGGREGATIONS] 
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

	-- Daily calculation of solar customer interval >10kw
	DELETE FROM MDM.INTERVAL_READ_EXCEEDING_10KW WHERE interval_end_dt_tm <= CONVERT (DATETIME, DATEDIFF(DAY, 45, GETDATE()))
	DELETE FROM MDM.INTERVAL_READ_EXCEEDING_10KW WHERE interval_end_dt_tm > CONVERT (DATETIME, DATEDIFF(DAY, 2, GETDATE()))

	INSERT INTO MDM.INTERVAL_READ_EXCEEDING_10KW (INTERVAL_END_DT_TM, BI_MTR_NBR, VALUE)
	 SELECT  ir.interval_end_dt_tm, m.bi_mtr_nbr, ir.value * 4
		FROM 
			[NISC].bi_srv_loc_hist_installed_meters_view m INNER JOIN mdm.interval_read_view ir ON m.bi_mtr_nbr = ir.bi_mtr_nbr
		WHERE
			m.bi_net_meter_sw = 'Y'
			AND ir.uom_nbr = (SELECT uom_nbr FROM mdm.dim_uom_view WHERE uom = 'KWH_USAGE' AND channel = 2)
			AND ir.interval_end_dt_tm > CONVERT (DATETIME, DATEDIFF(DAY, 2, GETDATE()))
			AND ((ir.value * 4)/m.bi_ct_ratio) > 10


END

