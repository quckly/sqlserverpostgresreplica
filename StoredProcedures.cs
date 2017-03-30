using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Data.Odbc;
using System.Net;
using System.Text;

namespace SqlServerPostgresIntegration
{
    public partial class StoredProcedures
    {
        //public static void ExecutePgsql(OdbcCommand cmd)
        //{
        //    using (var conn = new OdbcConnection("DSN=PostgreSQL30;")) //"Driver={PostgreSQL ANSI};Server=localhost;Port=5432;Database=db5;Uid=postgres;Pwd=postgres;")) // PostgreSQL30 // DEVARTPG
        //    {
        //        //throw new Exception(cmd.CommandText);
        //        conn.Open();
        //        cmd.Connection = conn;
        //        cmd.ExecuteNonQuery();
        //    }
        //}

        [SqlTrigger(Name = @"ReplicaToPg", Target = "db5", Event = "AFTER INSERT, UPDATE, DELETE")]
        public static void ReplicaToPg()
        {
            SqlCommand command;
            SqlTriggerContext triggContext = SqlContext.TriggerContext;
            SqlDataReader reader;

            // Retrieve the connection that the trigger is using  
            using (SqlConnection connection = new SqlConnection(@"context connection=true"))
            {
                connection.Open();

                SqlCommand cmdTableName =
                    new SqlCommand(
                        "SELECT object_name(resource_associated_entity_id) FROM sys.dm_tran_locks WHERE request_session_id = @@spid and resource_type = 'OBJECT'",
                        connection);
                var tableName = cmdTableName.ExecuteScalar().ToString();

                switch (triggContext.TriggerAction)
                {
                    case TriggerAction.Insert:
                        {
                            command = new SqlCommand(@"SELECT * FROM INSERTED;", connection);
                            reader = command.ExecuteReader();

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    StringBuilder sb = new StringBuilder($"INSERT INTO {tableName} (");

                                    for (int columnNumber = 0; columnNumber < triggContext.ColumnCount; columnNumber++)
                                    {
                                        if (columnNumber > 0)
                                        {
                                            sb.Append(", ");
                                        }

                                        sb.Append(reader.GetName(columnNumber));
                                    }
                                    sb.Append(") VALUES (");

                                    for (int columnNumber = 0; columnNumber < triggContext.ColumnCount; columnNumber++)
                                    {
                                        if (columnNumber > 0)
                                        {
                                            sb.Append(", ");
                                        }

                                        //sb.Append("?");
                                        if (reader.GetDataTypeName(columnNumber) == "int")
                                        {
                                            sb.Append(
                                                $"{reader.GetSqlValue(columnNumber).ToString()}");
                                        }
                                        else
                                        {
                                            sb.Append(
                                                $"'{reader.GetSqlValue(columnNumber).ToString().Replace("'", "''")}'");
                                        }
                                    }

                                    sb.Append(")");

                                    //using (var cmd = new OdbcCommand(sb.ToString()))
                                    //{
                                    //    for (int columnNumber = 0; columnNumber < triggContext.ColumnCount; columnNumber++)
                                    //    {
                                    //        var param = new OdbcParameter();
                                    //        param.OdbcType = (OdbcType)Enum.Parse(typeof(OdbcType), reader.GetDataTypeName(columnNumber), true);
                                    //        param.Value = reader.GetSqlValue(columnNumber);
                                    //        cmd.Parameters.Add(param);
                                    //    }

                                    //    ExecutePgsql(cmd);
                                    //}

                                    WebClient client = new WebClient();
                                    client.UploadString("http://192.168.1.10:8080/", sb.ToString());
                                }

                                reader.Close();
                            }

                            break;
                        }

                    case TriggerAction.Update:
                        {
                            command = new SqlCommand(@"SELECT * FROM INSERTED;", connection);
                            reader = command.ExecuteReader();
                            reader.Read();

                            for (int columnNumber = 0; columnNumber < triggContext.ColumnCount; columnNumber++)
                            {
                                //pipe.Send("Updated column "
                                //            + reader.GetName(columnNumber) + "? "
                                //            + triggContext.IsUpdatedColumn(columnNumber).ToString());
                            }

                            reader.Close();

                            break;
                        }

                    case TriggerAction.Delete:
                        {
                            command = new SqlCommand(@"SELECT * FROM DELETED;", connection);
                            reader = command.ExecuteReader();

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {

                                }

                                reader.Close();
                            }

                            break;
                        }
                }
            }
        }

        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void HelloWorld()
        {
            Microsoft.SqlServer.Server.SqlMetaData columnInfo
                    = new Microsoft.SqlServer.Server.SqlMetaData("Column1", SqlDbType.NVarChar, 12);
            SqlDataRecord greetingRecord
                = new SqlDataRecord(new Microsoft.SqlServer.Server.SqlMetaData[] { columnInfo });
            greetingRecord.SetString(0, "Hello world!");
            SqlContext.Pipe.Send(greetingRecord);
        }
    };
}
