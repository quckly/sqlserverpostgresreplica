using System;
using System.Data.Odbc;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace TestPgOdbc
{
    class Program
    {
        public static void ExecutePgsql(OdbcCommand cmd)
        {
            try
            {
                using (var conn = new OdbcConnection("DSN=PostgreSQL35W;"))
                    //"Driver={PostgreSQL ANSI};Server=localhost;Port=5432;Database=db5;Uid=postgres;Pwd=postgres;")) // PostgreSQL30 // DEVARTPG
                {
                    conn.Open();
                    cmd.Connection = conn;
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        static void Main(string[] args)
        {
            Run();
        }

        public static void Run()
        {
            var web = new HttpListener();

            web.Prefixes.Add("http://*:8080/");
            web.Start();
            
            while (web.IsListening)
            {
                var context = web.GetContext();

                var query = ReadStringRequest(context.Request).Result;

                using (var cmd = new OdbcCommand(query))
                {
                    ExecutePgsql(cmd);
                }

                context.Response.ContentType = "text/html";
                context.Response.StatusCode = 200;

                context.Response.OutputStream.Close();
            }
        }

        public static async Task<string> ReadStringRequest(HttpListenerRequest request)
        {
            if (request.InputStream.CanRead)
            {
                using (var ms = new MemoryStream())
                {
                    await request.InputStream.CopyToAsync(ms);

                    return Encoding.UTF8.GetString(ms.ToArray());
                }
            }

            return null;
        }

        //public static void Run()
        //{
        //    var listener = new TcpListener(IPAddress.Any, 8080);
        //    listener.Start();
        //    while (true)
        //    {
        //        byte[] buff = new byte[10024];
        //        MemoryStream ms = new MemoryStream();
        //        TcpClient s = listener.AcceptTcpClient();

        //        int Count;
        //        //while ((Count = s.GetStream().Read(buff, 0, buff.Length)) > 0)
        //        Count = s.GetStream().Read(buff, 0, buff.Length);
        //        {
        //            ms.Write(buff, 0, Count);
        //        }

        //        var httpRequest = Encoding.UTF8.GetString(ms.ToArray());
        //        var id = httpRequest.IndexOf("\r\n\r\n");
        //        var query = httpRequest.Substring(id + 4);

        //        using (var cmd = new OdbcCommand(query))
        //        {
        //            ExecutePgsql(cmd);
        //        }
        //    }
        //}
    }
}
