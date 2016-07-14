using System;
using System.Collections.Generic;
using System.Text;

namespace DBSync3
{
    class Comment : ISchemaElement
    {
        private string type;
        private string what;
        private string on;
        private string comment;

        public Comment(string typ, string wht, string ona, string com)
        {
            type = typ;
            what = wht;
            on = ona;
            comment = com;
        }

        public void set_type(string typ)
        {
            type = typ;
        }

        public string get_type()
        {
            return type;
        }

        public string get_name()
        {
            return type + what + on;
        }

        public void add_postgres()
        {
            if (type == "CONSTRAINT")
                Console.WriteLine("COMMENT ON " + type + " \"" + what + "\" ON \"" + on + "\" IS " + comment + ";\n");
            else
                Console.WriteLine("COMMENT ON " + type + " " + DBSync3.quote_two_names(what) + " IS " + comment + ";\n");
        }

        public void add_microsoft()
        {
        }

        public void add_oracle()
        {
            if (type != "CONSTRAINT"  &&  type != "SCHEMA")
                Console.WriteLine("COMMENT ON " + type + " " + DBSync3.quote_two_names(what).ToUpper() + " IS " + comment + ";\n");
        }

        public void drop_postgres()
        {
        }

        public void drop_microsoft()
        {
        }

    }
}
