USE [MDMDW]
GO

/****** Object:  View [NISC].[BI_SRV_LOC_HIST_INSTALLED_METERS_VIEW]    Script Date: 7/3/2024 11:21:40 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



ALTER VIEW [NISC].[BI_SRV_LOC_HIST_INSTALLED_METERS_VIEW]
	AS
	WITH cte AS
(
	SELECT h.*, l.bi_addr1, l.bi_addr2, l.bi_addr3, l.bi_city, l.bi_st, l.bi_zip,
			t.bi_trf_nbr, el.bi_fdr, el.bi_sub, l.BI_DIST_OFC_CD,
			TRIM(el.bi_pri_phs) AS bi_pri_phs, TRIM(el.bi_sec_phs) AS bi_sec_phs,
			s.bi_map_loc_nbr, 
			l.bi_srv_map_loc,
			rcr.bi_rev_class_desc, 
			ms.bi_ct_ratio,
			ms.bi_pt_ratio,
			ms.bi_net_meter_sw, 
			l.bi_x_coord,
			l.bi_y_coord,
			c.bi_acct_stat_cd,
			mi.bi_amr_transponder_id,
			(CASE l.bi_dist_ofc_cd WHEN 2 THEN 'SEDALIA'
				WHEN 4 THEN 'BENNETT'
				WHEN 5 THEN 'CONIFER'
				WHEN 1 THEN 'WOODLAND PARK'
				ELSE '' END) AS bi_service_area, 
			-- GPT added on 11/30/2023
			mi.BI_MTR_FORM_NBR,
			-- GPT added on 1/18/2024
			ms.BI_MTR_NOM_VOLTS_1,
			-- GPT added on 3/21/2024
			l.BI_LMN_SRV_AREA,
			-- GPT added on 4/30/2024
			l.BI_SUBD_NAME,
		--ROW_NUMBER() OVER (PARTITION BY h.bi_srv_loc_nbr ORDER BY h.bi_hist_dt DESC) AS 
		ROW_NUMBER() OVER (PARTITION BY h.bi_mtr_nbr ORDER BY h.bi_hist_dt DESC) AS 
		rn
	FROM [NISC].bi_srv_loc_hist h
		INNER JOIN [NISC].bi_srv_loc l ON h.bi_srv_loc_nbr = l.bi_srv_loc_nbr
		--INNER JOIN [NISC].bi_srv_link s ON h.bi_srv_loc_nbr = s.bi_srv_loc_nbr AND h.bi_mtr_pos_nbr = s.bi_mtr_pos_nbr
		INNER JOIN [NISC].bi_srv_link s ON h.bi_srv_loc_nbr = s.bi_srv_loc_nbr AND h.bi_mtr_nbr = s.bi_mtr_nbr
		-- GPT updated on 07/03/2024
		--INNER JOIN [NISC].bi_trf_link t ON t.bi_map_loc_nbr = s.bi_map_loc_nbr
		INNER JOIN [NISC].bi_trf_link t ON t.bi_map_loc_nbr = s.bi_map_loc_nbr AND t.BI_TRF_NBR = s.BI_TRF_NBR
		INNER JOIN [NISC].bi_equip_loc el ON el.bi_map_loc_nbr = s.bi_map_loc_nbr
		INNER JOIN [NISC].bi_type_service ts ON h.bi_acct = ts.bi_acct
		INNER JOIN [NISC].bi_rev_class_ref rcr ON rcr.bi_type_srv = ts.bi_type_srv AND rcr.bi_rev_class_cd = ts.bi_rev_class_cd
		--INNER JOIN [NISC].bi_mtr_srv ms ON h.bi_srv_loc_nbr = ms.bi_srv_loc_nbr AND h.bi_mtr_pos_nbr = ms.bi_mtr_pos_nbr
		INNER JOIN [NISC].bi_mtr_srv ms ON h.bi_srv_loc_nbr = ms.bi_srv_loc_nbr AND h.bi_mtr_nbr = ms.bi_mtr_nbr
		INNER JOIN [NISC].bi_consumer c ON h.bi_acct = c.bi_acct AND h.bi_cust_nbr = c.bi_cust_nbr
		INNER JOIN [NISC].bi_mtr_inv mi ON h.bi_mtr_nbr = mi.bi_mtr_nbr
	WHERE 
		h.bi_event_cd = 'MTI'

)
SELECT * FROM cte WHERE rn = 1

GO


