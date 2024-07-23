USE [MDMDW]
GO

/****** Object:  Table [MDM].[D_AGGREGATE_ATTRIBUTE]    Script Date: 6/21/2024 9:34:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [MDM].[D_AGGREGATE_ATTRIBUTE](
	[AGGREGATE_ATTRIBUTE_ID] [int] IDENTITY(1,1) NOT NULL,
	[AGGREGATE_ATTRIBUTE_TYPE] [varchar](40) NOT NULL,
	[AGGREGATE_ATTRIBUTE_VALUE] [varchar](30) NOT NULL,
	[AGGREGATE_ATTRIBUTE_PHASE] [varchar](3) NULL,
 CONSTRAINT [PK_D_AGGREGATE_ATTRIBUTE] PRIMARY KEY CLUSTERED 
(
	[AGGREGATE_ATTRIBUTE_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 100, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

