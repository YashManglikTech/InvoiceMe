using System;

namespace SampleApp
{
    class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }

        public void Greet()
        {
            Console.WriteLine($"Hello, my name is {Name} and I'm {Age} years old.");
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Person person = new Person();
            person.Name = "Alice";
            person.Age = 30;
            person.Greet();
        }
    }
}
