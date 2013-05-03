using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DMSLogParser
{
    class Program
    {
        static void Main(string[] args)
        {
            string logFilePath = string.Empty; // Set it before running the app.
            DMSLogParser.BeginParsing(logFilePath);
        }
    }
}
