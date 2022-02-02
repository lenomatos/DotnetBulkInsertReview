using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ConsoleApplication1
{
    class Program
    {

        static void Main()
        {

            string connString = ConfigurationManager.ConnectionStrings["Bulk"].ConnectionString;
            string filePath = ConfigurationManager.AppSettings["FilePath"];

            using (SqlConnection conn = new SqlConnection(connString))
            {
                conn.Open();
                using (SqlTransaction tran = conn.BeginTransaction())
                {

                    Insert(MapDT(), ReadCSV(filePath), conn, tran);
                    tran.Commit();
                }
            }

        }

        static private DataTable MapDT()
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("FirstName", typeof(string));
            dataTable.Columns.Add("MiddleName", typeof(string));
            dataTable.Columns.Add("LastName", typeof(string));
            dataTable.Columns.Add("GraduationYear", typeof(int));

            return dataTable;
        }

        static private List<Student> ReadCSV(string filePath)
        {
            bool isHeader = true;

            List<Student> students = new List<Student>();

            using (StreamReader sr = new StreamReader(filePath, Encoding.UTF8))
            {
                string line = "";

                while ((line = sr.ReadLine()) != null)
                {

                    if (isHeader)
                    {
                        isHeader = false;
                        continue;
                    }

                    String[] fields = line.Split(",");

                    if (!String.IsNullOrEmpty(fields[0].Trim()))
                    {

                        var student = new Student();
                        student.FirstName = fields[2];
                        student.MiddleName = fields[3];
                        student.LastName = fields[9];
                        student.GraduationYear = String.IsNullOrEmpty(fields[13]) ? 0 : Convert.ToInt16(fields[13]);
                        students.Add(student);
                    }

                }
            }


            return students;
        }

        static private void Insert(DataTable studentsTable, List<Student> students, SqlConnection conn, SqlTransaction tran)
        {
            for (int i = 0; i < students.Count; i++)
            {
                studentsTable.Rows.Add(new object[] {
                            students[i].FirstName,
                            students[i].MiddleName,
                            students[i].LastName,
                            students[i].GraduationYear
                });
            }

            using (SqlBulkCopy bulkCopy =
                    new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran))
            {

                bulkCopy.DestinationTableName = "Students";
                bulkCopy.ColumnMappings.Add("FirstName", "FirstName");
                bulkCopy.ColumnMappings.Add("MiddleName", "MiddleName");
                bulkCopy.ColumnMappings.Add("LastName", "LastName");
                bulkCopy.ColumnMappings.Add("GraduationYear", "GraduationYear");
                bulkCopy.BulkCopyTimeout = 300000;
                bulkCopy.WriteToServer(studentsTable);

            }
        }

        public class Student
        {
            public string FirstName { get; set; }
            public string MiddleName { get; set; }
            public string LastName { get; set; }
            public int GraduationYear { get; set; }

        }

    }
}