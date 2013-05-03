using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Text.RegularExpressions;
namespace DMSLogParser
{
	public class DMSLogParser
	{

		//static Regex regex1 = new Regex(@"(?<Msg1>Deal ID : DMS:Deal:\s+))(?<DealId>[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})", RegexOptions.Compiled);
		static Regex regex1 = new Regex(@"(?<msg1>Deal ID : DMS:Deal:(\s{0,2}))(?<DealId>[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})", RegexOptions.Compiled);
		static Regex regex2 = new Regex(@"(?<Msg1>Deal ID : )(?<DealId>[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})", RegexOptions.Compiled);
		static Regex regex3 = new Regex(@"(?<msg1>Deal ID :)(?<DealId>[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})", RegexOptions.Compiled);
		public static void BeginParsing(string fileName_)
		{
			int index = 0;
			string line = string.Empty;
			string actualLine = string.Empty;
			string[] splitter = ConfigurationManager.AppSettings["SplitterText"].Split(new char[] { ',' });
			LogMessageOnConsole("Opening Stream...");
			using (StreamReader reader = new StreamReader(fileName_))
			{
				line = reader.ReadLine();
				string[] wordArray = null;
				DataTable dt = GetSQLTableSchema();
				while (string.IsNullOrEmpty(line) == false)
				{
					index++;
					try
					{
						if (line.Contains("Node Data :") == false)
						{
							actualLine = line;
							foreach (var item in splitter)
							{
								line = line.Replace(item, "|" + item);
							}
							wordArray = line.Split(new char[] { '|' });
							if (wordArray != null && wordArray.Length > 2)
							{
								if (wordArray[2].Trim().StartsWith("No Record Found for query :") == false)
								{
									Match match = regex1.Match(wordArray[2]);
									if (match.Success)
									{
										dt.Rows.Add(new object[] { index, wordArray[0], match.Groups["DealId"].Value, wordArray[2].Replace(match.ToString(), string.Empty), wordArray[3] });
									}
									else
									{
										match = regex2.Match(wordArray[2]);
										if (match.Success)
										{
											dt.Rows.Add(new object[] { index, wordArray[0], match.Groups["DealId"].Value, null, wordArray[2].Replace(match.Value.ToString(), string.Empty) });
										}
										else
										{
											match = regex3.Match(wordArray[2]);
											if (match.Success)
											{
												dt.Rows.Add(new object[] { index, wordArray[0], match.Groups["DealId"].Value, wordArray[2].Replace(match.Value.ToString(), string.Empty), wordArray[3] });
											}
											else
											{
												LogMessageOnConsole("No Match found for line : " + line);
											}
										}
									}
								}
							}
						}
					}
					catch (Exception ex)
					{
						LogMessageOnConsole("Error while reading line " + line + " : " + ex.Message + "\r\n");
					}

					if (dt.Rows.Count >= 1000)
					{
						LogMessageOnConsole("Going to save in db.");
						DumpInDb(dt);
						LogMessageOnConsole("DB Save completed.");
					}
					line = reader.ReadLine();
				}
				if (dt.Rows.Count > 0)
				{
					DumpInDb(dt);
				}
			}
		}

		private static void DumpInDb(DataTable dt)
		{
			SqlBulkCopy bc = new SqlBulkCopy(ConfigurationManager.ConnectionStrings["SqlConn"].ConnectionString);
			bc.DestinationTableName = "DMSLog";
			bc.ColumnMappings.Add("LineNumber", "LineNumber");
			bc.ColumnMappings.Add("LogDateTime", "LogDateTime");
			bc.ColumnMappings.Add("DealId", "DealId");
			bc.ColumnMappings.Add("Params", "Params");
			bc.ColumnMappings.Add("DMSRemark", "DMSRemark");
			bc.WriteToServer(dt);
			dt.Rows.Clear();
			dt.AcceptChanges();
		}
		static DataTable GetSQLTableSchema()
		{
			DataTable dt = new DataTable();
			dt.Columns.Add("LineNumber");
			dt.Columns.Add("LogDateTime");
			dt.Columns.Add("DealId");
			dt.Columns.Add("Params");
			dt.Columns.Add("DMSRemark");
			return dt;
		}

		private static void LogMessageOnConsole(string p)
		{
			Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss:fff") + " >> " + p);
		}

	}
}
 