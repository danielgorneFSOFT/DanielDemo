using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;

namespace DanielDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                string connectionString = "server = " + args[0] + ";"
                                   + "user id = " + args[1] + ";"
                                   + "password = " + args[2] + ";" 
                                   + "MultipleActiveResultSets=true";

                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    //connect to server
                    try
                    {
                        conn.Open();
                        //INITIALIZE PARAMETERS

                        Console.Write("update within: ");
                        int update_within = Convert.ToInt32(Console.ReadLine());

                        Console.Write("config db: ");
                        string config_db = Console.ReadLine();

                        Console.Write("db list: ");
                        string db_list = Console.ReadLine();

                        Console.Write("table list: ");
                        string table_list = Console.ReadLine();

                        String storageConnection = System.Configuration.ConfigurationManager.AppSettings.Get("StorageConnectionString");
                        CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnection);

                        Ingester ingester = new Ingester(conn, storageAccount, update_within, config_db, db_list, table_list);
                        ingester.Start();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }         
        }
    }

    class Ingester
    {
        private SqlConnection conn;
        private CloudStorageAccount storageAccount;
        private int update_within;
        private string config_db;
        private string db_list;
        private string table_list;

        public Ingester(SqlConnection conn, CloudStorageAccount storageAccount, int update_within, string config_db, string db_list, string table_list)
        {
            this.conn = conn;
            this.storageAccount = storageAccount;
            this.update_within = update_within;
            this.config_db = config_db;
            this.db_list = db_list;
            this.table_list = table_list;
        }

        public void Start()
        {
            //INITIALIZE BY RETRIEVING THE LIST OF DATABASE AND TABLES
            using (SqlCommand queryForDatabase = new SqlCommand("SELECT * FROM " + config_db  + ".dbo." + db_list, conn))
            {
                using (SqlDataReader databaseReader = queryForDatabase.ExecuteReader())
                {
                    while (databaseReader.Read())
                    {
                        using (SqlCommand queryForTables = new SqlCommand("SELECT * FROM " + config_db + ".dbo." + table_list + " WHERE id = " + databaseReader[0], conn))
                        {
                            using (SqlDataReader tableReader = queryForTables.ExecuteReader())
                            {
                                while (tableReader.Read())
                                {
                                    //CHECK FOR UPDATES
                                    CheckUpdates((string) databaseReader[1], (string) tableReader[1]);
                                }
                            }
                        }
                    }
                }
            }
        }

        private void CheckUpdates(string database, string table)
        {
            using (SqlCommand queryForTimeStamps = new SqlCommand("SELECT MAX(TimeStamp), GETUTCDATE() FROM " + database + ".dbo." + table, conn))
            {
                using (SqlDataReader timeStampReader = queryForTimeStamps.ExecuteReader())
                {
                    while (timeStampReader.Read())
                    {
                        CloudBlockBlob blockBlob = storageAccount.CreateCloudBlobClient().
                                                    GetContainerReference("csv").
                                                    GetBlockBlobReference(database + "/" + table + ".csv");

                        //CHECK IF BLOB EXISTS, IF IT DOES UPLOAD IT FIRST THEN CHECK FOR LAST MODIFIED
                        if (blockBlob.Exists())
                        {
                            if ((Convert.ToDateTime(timeStampReader[0]) - Convert.ToDateTime(Convert.ToString(blockBlob.Properties.LastModified)).ToLocalTime()).TotalMinutes > update_within)
                            {
                                UploadToAzure(blockBlob, database, table);
                            }
                            else
                            {
                                Console.WriteLine("DON'T UPLOAD");
                            }

                            Console.WriteLine("-----------------");
                            Console.WriteLine("BLOB DATE: " + (Convert.ToDateTime(Convert.ToString(blockBlob.Properties.LastModified)).ToLocalTime()));
                            Console.WriteLine("SQL DATE: " + timeStampReader[0]);
                        }
                        else
                        {
                            UploadToAzure(blockBlob, database, table);
                        }
                    }
                }
            }
        }

        private void UploadToAzure(CloudBlockBlob blockBlob, string database, string table)
        {
            Console.WriteLine("------");
            Console.WriteLine("SAVE AS .CSV THEN UPLOAD TO BLOB STORAGE");
            Console.WriteLine("------");

            //EXPORT AS CSV
            using (SqlCommand queryForContent = new SqlCommand("SELECT * FROM " + database + ".dbo." + table, conn))
            {
                using (SqlDataReader contentReader = queryForContent.ExecuteReader())
                {
                    try
                    {
                        Directory.CreateDirectory(Path.Combine(Directory.GetCurrentDirectory() + @"\" + database));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                    }

                    using (StreamWriter file_writer = new StreamWriter(Path.Combine(Directory.GetCurrentDirectory() + @"\" + database + @"\" + table + ".csv")))
                    {
                        object[] output = new object[contentReader.FieldCount];

                        for (int i = 0; i < contentReader.FieldCount; i++)
                        {
                            output[i] = contentReader.GetName(i);
                        }

                        while (contentReader.Read())
                        {
                            file_writer.WriteLine(string.Join(",", output));

                            contentReader.GetValues(output);
                            file_writer.WriteLine(string.Join(",", output));
                        }
                    }
                }
            }

            //UPLOAD TO AZURE
            using (var fileStream = System.IO.File.OpenRead(Path.Combine(Directory.GetCurrentDirectory() + @"\" + database + @"\" + table + ".csv")))
            {
                blockBlob.UploadFromStream(fileStream);
            }
        }
    }
}
