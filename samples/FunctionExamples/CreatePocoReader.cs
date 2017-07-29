using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.ComTypes;

namespace FunctionExamples
{
    public class CreatePocoReader
    {
        public List<BookClass> CreateSampleData()
        {
            var books = new List<BookClass>();
            
            books.Add(new BookClass() {Code = "001", Name = "Lord of the rings", Cost = 15, Published = new DateTime(1960, 01,01)});
            books.Add(new BookClass() {Code = "002", Name = "Harry Potter", Cost = 12, Published = new DateTime(1995, 06,01)});
            books.Add(new BookClass() {Code = "003", Name = "Game of Thrones", Cost = 16, Published = new DateTime(1996, 06,01)});

            return books;
        }

        public CreatePocoReader()
        {
        }
    }
}