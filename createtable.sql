
CREATE TABLE [dbo].[DMSLog](
	[LogDateTime] [datetime] NULL,
	[DealId] [varchar](100) NULL,
	[Params] [varchar](4000) NULL,
	[LineNumber] [int] NULL,
	[DMSRemark] [varchar](4000) NULL
) ON [PRIMARY]