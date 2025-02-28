USE [MDMDW]
GO

/****** Object:  Table [NISC].[D_METER_FILTER]    Script Date: 6/21/2024 9:33:39 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [NISC].[D_METER_FILTER](
	[BI_MTR_NBR] [varchar](15) NOT NULL,
	[AGGREGATE_ATTRIBUTE_TYPE] [varchar](30) NOT NULL,
 CONSTRAINT [PK_D_METER_FILTER] PRIMARY KEY CLUSTERED 
(
	[BI_MTR_NBR] ASC,
	[AGGREGATE_ATTRIBUTE_TYPE] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 100, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

