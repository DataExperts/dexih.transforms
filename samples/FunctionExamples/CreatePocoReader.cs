using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Runtime.InteropServices.ComTypes;
using dexih.transforms;
using dexih.transforms.tests;

namespace FunctionExamples
{
    public class CreatePocoReader
    {
        public void Create()
        {
            var books = CreatBooksData();
            var reader = new ReaderPoco<BookClass>(books);

            DisplayReader(reader);
        }
        
        public List<BookClass> CreatBooksData()
        {
            var books = new List<BookClass>();
            
            books.Add(new BookClass() {Code = "001", Name = "Lord of the rings", Cost = 15, Published = new DateTime(1954, 07,29)});
            books.Add(new BookClass() {Code = "002", Name = "Harry Potter and the Philosopher's Stone", Cost = 12, Published = new DateTime(1997, 06,26)});
            books.Add(new BookClass() {Code = "003", Name = "A Game of Thrones", Cost = 16, Published = new DateTime(1996, 07,01)});

            return books;
        }

        public void DisplayReader(DbDataReader reader)
        {
            while (reader.Read())
            {
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    Console.Write(reader.GetName(i) + ":" + reader[i].ToString() + (i < reader.FieldCount-1 ? ", " : ""));
                }
                Console.WriteLine();
            }
        }

    }
}