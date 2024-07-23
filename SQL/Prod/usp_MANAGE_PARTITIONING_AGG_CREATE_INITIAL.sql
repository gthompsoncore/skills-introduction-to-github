USE [MDMDW]
GO

/****** Object:  StoredProcedure [MDM].[usp_MANAGE_PARTITIONING_AGG_CREATE_INITIAL]    Script Date: 7/11/2024 12:33:56 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--exec [MDM].[usp_MANAGE_PARTITIONING_AGG_CREATE_INITIAL]

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [MDM].[usp_MANAGE_PARTITIONING_AGG_CREATE_INITIAL]
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @PartitionList TABLE (Value NVARCHAR(14))
	
	-- Create the file groups for each month.
	
	INSERT INTO @PartitionList VALUES ('FG_AGG_01_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_02_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_03_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_04_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_05_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_06_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_07_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_08_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_09_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_10_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_11_2023')
	INSERT INTO @PartitionList VALUES ('FG_AGG_12_2023')
	
	INSERT INTO @PartitionList VALUES ('FG_AGG_01_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_02_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_03_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_04_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_05_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_06_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_07_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_08_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_09_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_10_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_11_2024')
	INSERT INTO @PartitionList VALUES ('FG_AGG_12_2024')

	INSERT INTO @PartitionList VALUES ('FG_AGG_01_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_02_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_03_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_04_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_05_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_06_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_07_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_08_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_09_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_10_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_11_2025')
	INSERT INTO @PartitionList VALUES ('FG_AGG_12_2025')

	DECLARE @PartitionName varchar(14)
	DECLARE @filegroup NVARCHAR(MAX) = ''
	DECLARE @file NVARCHAR(MAX) = ''

	DECLARE cur CURSOR FOR SELECT Value FROM @PartitionList
	OPEN cur

	FETCH NEXT FROM cur INTO @PartitionName

	-- Add the file groups to the database and add the files to each file group
	WHILE @@FETCH_STATUS = 0 BEGIN
		-- Build the strings for the commands
		SET @filegroup = @filegroup + CONCAT('ALTER DATABASE MDMDW ADD FILEGROUP ', @PartitionName)

		SET @file = @file + CONCAT('
			ALTER DATABASE MDMDW
			ADD FILE
			(
			  NAME = [',@PartitionName,'],
			  FILENAME = ''G:\\DATA\\',@PartitionName,'.ndf'', 
				SIZE = 5 MB,  
				MAXSIZE = UNLIMITED, 
				FILEGROWTH = 10 MB
			) TO FILEGROUP ',@PartitionName)

			-- excecute the commands
			exec (@filegroup)
			exec (@file)

			PRINT(@filegroup)
			PRINT(@file)

			-- reset the strings
			SET @filegroup = ''
			SET @file = ''

		FETCH NEXT FROM cur INTO @PartitionName
	END

	CLOSE cur    
	DEALLOCATE cur

	-- Add the Partition Function with Month wise range
	-- DROP PARTITION FUNCTION [PF_MonthlyPartition]
	CREATE PARTITION FUNCTION [PF_AGG_MonthlyPartition] (DATETIME)
	AS RANGE RIGHT FOR VALUES 
	(
	  '2023-01-01 00:00:00.000', '2023-02-01 00:00:00.000', '2023-03-01 00:00:00.000', '2023-04-01 00:00:00.000', '2023-05-01 00:00:00.000', '2023-06-01 00:00:00.000', '2023-07-01 00:00:00.000', '2023-08-01 00:00:00.000', '2023-09-01 00:00:00.000', '2023-10-01 00:00:00.000', '2023-11-01 00:00:00.000', '2023-12-01 00:00:00.000',
	  '2024-01-01 00:00:00.000', '2024-02-01 00:00:00.000', '2024-03-01 00:00:00.000', '2024-04-01 00:00:00.000', '2024-05-01 00:00:00.000', '2024-06-01 00:00:00.000', '2024-07-01 00:00:00.000', '2024-08-01 00:00:00.000', '2024-09-01 00:00:00.000', '2024-10-01 00:00:00.000', '2024-11-01 00:00:00.000', '2024-12-01 00:00:00.000',
	  '2025-01-01 00:00:00.000', '2025-02-01 00:00:00.000', '2025-03-01 00:00:00.000', '2025-04-01 00:00:00.000', '2025-05-01 00:00:00.000', '2025-06-01 00:00:00.000', '2025-07-01 00:00:00.000', '2025-08-01 00:00:00.000', '2025-09-01 00:00:00.000', '2025-10-01 00:00:00.000', '2025-11-01 00:00:00.000', '2025-12-01 00:00:00.000'
	)

	-- Add the Partition Scheme with File Groups to the Partition Function
	CREATE PARTITION SCHEME PS_AGG_MonthWise
	AS PARTITION PF_AGG_MonthlyPartition
	TO 
	( 
	  'FG_AGG_01_2023', 'FG_AGG_02_2023', 'FG_AGG_03_2023', 'FG_AGG_04_2023', 'FG_AGG_05_2023', 'FG_AGG_06_2023', 'FG_AGG_07_2023', 'FG_AGG_08_2023', 'FG_AGG_09_2023', 'FG_AGG_10_2023', 'FG_AGG_11_2023', 'FG_AGG_12_2023', 
	  'FG_AGG_01_2024', 'FG_AGG_02_2024', 'FG_AGG_03_2024', 'FG_AGG_04_2024', 'FG_AGG_05_2024', 'FG_AGG_06_2024', 'FG_AGG_07_2024', 'FG_AGG_08_2024', 'FG_AGG_09_2024', 'FG_AGG_10_2024', 'FG_AGG_11_2024', 'FG_AGG_12_2024', 
	  'FG_AGG_01_2025', 'FG_AGG_02_2025', 'FG_AGG_03_2025', 'FG_AGG_04_2025', 'FG_AGG_05_2025', 'FG_AGG_06_2025', 'FG_AGG_07_2025', 'FG_AGG_08_2025', 'FG_AGG_09_2025', 'FG_AGG_10_2025', 'FG_AGG_11_2025', 'FG_AGG_12_2025', 
	  'Primary'
	)

	-- Create the Tables with the Partition Scheme
	-- Do this manually, there is only 1 table needing it - 
	-- Partition them as follows
		-- [MDM].[F_INTERVAL_READ_BY_ATTRIBUTE] on [INTERVAL_END_DT_TM]
		
	-- For example below, also make sure to script any indexes that get dropped and recreate them using the partition
	--CREATE TABLE orders
	--(
	--  [order_id] BIGINT IDENTITY(1,1) NOT NULL,
	--  [user_id] BIGINT,
	--  [order_amt] DECIMAL(10,2),
	--  [address_id] BIGINT,
	--  [status_id] TINYINT,
	--  [is_active] BIT,
	--  [order_date] [datetime]
	--) ON PS_MonthWise ([order_date]);
	--CREATE CLUSTERED INDEX CI_orders_order_id ON orders(order_id)
	
END
GO


