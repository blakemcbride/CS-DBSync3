using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace DBSync3
{
    class DBSync3
    {
        public static string dashes = "\n\n-- ----------------------------------------------------------\n";

        private static bool fk_reset = true;

        static int Main(string[] args)
        {
            if (args.Length < 3)
                usage();
            Schema schema1 = null;
            Schema schema2 = null;
            bool compare = false;

            for (int arg = 0; arg < args.Length; arg++)
                if (args[arg] == "-nfk")
                    fk_reset = false;
                else if (args[arg] == "-ip")
                {
                    if (++arg == args.Length)
                        usage();
                    checkFile(args[arg]);
                    if (schema1 == null)
                    {
                        schema1 = ParsePostgres.parse_postgres_schema(args[arg]);
                        schema1 = sort_schema(schema1);
                    }
                    else
                    {
                        compare = true;
                        schema2 = ParsePostgres.parse_postgres_schema(args[arg]);
                        schema2 = sort_schema(schema2);
                    }
                }
                else if (args[arg] == "-im")
                {
                    if (++arg == args.Length)
                        usage();
                    checkFile(args[arg]);
                    if (schema1 == null)
                    {
                        schema1 = ParseMicrosoft.parse_microsoft_schema(args[arg]);
                        schema1 = sort_schema(schema1);
                    }
                    else
                    {
                        compare = true;
                        schema2 = ParseMicrosoft.parse_microsoft_schema(args[arg]);
                        schema2 = sort_schema(schema2);
                    }
                }
                else if (args[arg] == "-om")
                {
                    if (compare)
                    {
                        if (fk_reset)
                            generate_drop_foreign_keys_microsoft(schema1);
                        table_diff_microsoft(schema1, schema2);
                        if (fk_reset)
                            generate_foreign_keys_microsoft(schema2);
                    }
                    else
                        display_microsoft_schema(schema1);
                }
                else if (args[arg] == "-op")
                {
                    if (compare)
                    {
                        if (fk_reset)
                            generate_drop_foreign_keys_postgres(schema1);
                        table_diff_postgres(schema1, schema2);
                        if (fk_reset)
                            generate_foreign_keys_postgres(schema2);
                    }
                    else
                        display_postgres_schema(schema1);
                }
                else if (args[arg] == "-oo")
                {
                        display_oracle_schema(schema1);
                }
                else
                    usage();

            return 0;
        }

        private static void usage()
        {
            Console.Error.WriteLine("Usage:  DBSync3  -im|-ip  input-schema-file  -om|-op|-oo");
            Console.Error.WriteLine("          or");
            Console.Error.WriteLine("        DBSync3  [-nfk]  -im|-ip  old-schema-file  -im|-ip  new-schema-file  -om|-op");
            Environment.Exit(10);
        }

        private static void table_diff_postgres(Schema oldschema, Schema newschema)
        {
            Console.WriteLine("\n--  Remove indexes and checks\n");
            foreach (ISchemaElement olde in oldschema)
            {
                ISchemaElement newe = newschema.find(olde.get_name());
                if (newe == null)
                {
                    if (olde is Index || olde is Check)
                        olde.drop_postgres();
                }
                else if (newe is Index)
                    if ((Index)olde != (Index)newe)
                        olde.drop_postgres();
            }

            Console.WriteLine("\n--  Add new tables\n");
            foreach (ISchemaElement newe in newschema)
            {
                ISchemaElement olde = oldschema.find(newe.get_name());
                if (olde == null && newe is Table)
                    newe.add_postgres();
            }

            Console.WriteLine("\n--  Add new columns\n");
            foreach (ISchemaElement newe in newschema)
            {
                ISchemaElement olde = oldschema.find(newe.get_name());
                if (olde == null)
                    continue;
                else if (!Object.ReferenceEquals(newe.GetType(), olde.GetType()))
                {
                    Console.Error.WriteLine(newe.get_name() + " names two different types of objects.");
                    Environment.Exit(10);
                }
                else if (newe is Table)
                    foreach (ISchemaElement newcol in ((Table)newe).get_columns())
                    {
                        ISchemaElement oldcol = ((Table)olde).find(newcol.get_name());
                        if (oldcol == null)
                        {
                            if (!(newcol is Check))
                                newcol.add_postgres();  //  newcol is a Column
                        }
                        else if (!Object.ReferenceEquals(newcol.GetType(), oldcol.GetType()))
                        {
                            Console.Error.WriteLine(newcol.get_name() + " names two different types of objects.");
                            Environment.Exit(10);
                        }
                    }
            }

            Console.WriteLine("\n--  Change existing columns\n");
            foreach (ISchemaElement newe in newschema)
            {
                ISchemaElement olde = oldschema.find(newe.get_name());
                if (olde == null)
                    continue;
                else if (!Object.ReferenceEquals(newe.GetType(), olde.GetType()))
                {
                    Console.Error.WriteLine(newe.get_name() + " names two different types of objects.");
                    Environment.Exit(10);
                }
                else if (newe is Table)
                    foreach (ISchemaElement newcol in ((Table)newe).get_columns())
                    {
                        ISchemaElement oldcol = ((Table)olde).find(newcol.get_name());
                        if (oldcol == null)
                            continue;
                        else if (!Object.ReferenceEquals(newcol.GetType(), oldcol.GetType()))
                        {
                            Console.Error.WriteLine(newcol.get_name() + " names two different types of objects.");
                            Environment.Exit(10);
                        }
                        else if (newcol is Column)
                        {
                            if (((Column)newcol).get_default() != ((Column)oldcol).get_default())
                                Column.alter_default((Column)oldcol, (Column)newcol);
                            if ((Column)newcol != (Column)oldcol)
                            {
                                foreach (ISchemaElement index in oldschema)
                                    if (index is Index && ((Index)index).isElement(((Column)oldcol).get_name()))
                                        ((Index)index).drop_postgres();
                                Column.alter_column((Column)oldcol, (Column)newcol);
                            }
                        }
                    }
            }

            Console.WriteLine("\n--  Remove tables\n");
            foreach (ISchemaElement olde in oldschema)
            {
                ISchemaElement newe = newschema.find(olde.get_name());
                if (newe == null && olde is Table)
                    olde.drop_postgres();
            }

            Console.WriteLine("\n--  Drop columns\n");
            foreach (ISchemaElement olde in oldschema)
            {
                ISchemaElement newe = newschema.find(olde.get_name());
                if (newe == null)
                    continue;
                else if (!Object.ReferenceEquals(newe.GetType(), olde.GetType()))
                {
                    Console.Error.WriteLine(newe.get_name() + " names two different types of objects.");
                    Environment.Exit(10);
                }
                else if (newe is Table)
                    foreach (ISchemaElement oldcol in ((Table)olde).get_columns())
                    {
                        ISchemaElement newcol = ((Table)newe).find(oldcol.get_name());
                        if (newcol == null)
                        {
                            foreach (ISchemaElement chkcol in ((Table)olde).get_columns())
                                if (chkcol is Check && ((Check)chkcol).isElement(oldcol.get_name()))
                                    chkcol.drop_postgres();
                            oldcol.drop_postgres();
                        }
                    }
            }

            Console.WriteLine("\n--  Add new indexes and checks\n");
            foreach (ISchemaElement newe in newschema)
            {
                ISchemaElement olde = oldschema.find(newe.get_name());
                if (olde == null)
                {
                    if (!(newe is ForeignKey) && !(newe is Table))
                        newe.add_postgres();
                }
                else if (!Object.ReferenceEquals(newe.GetType(), olde.GetType()))
                {
                    Console.Error.WriteLine(newe.get_name() + " names two different types of objects.");
                    Environment.Exit(10);
                }
                else if (newe is Table)
                    foreach (ISchemaElement newcol in ((Table)newe).get_columns())
                    {
                        ISchemaElement oldcol = ((Table)olde).find(newcol.get_name());
                        if (oldcol == null)
                        {
                            if (newcol is Check)
                                newcol.add_postgres();  //  newcol is a Check
                        }
                        else if (!Object.ReferenceEquals(newcol.GetType(), oldcol.GetType()))
                        {
                            Console.Error.WriteLine(newcol.get_name() + " names two different types of objects.");
                            Environment.Exit(10);
                        }
                        else if (newcol is Check)
                        {
                            if ((Check)newcol != (Check)oldcol)
                                Check.alter_check((Check)oldcol, (Check)newcol);
                        }
                    }
                else if (newe is Index)
                    if ((Index)olde != (Index)newe)
                        newe.add_postgres();
            }

            Console.WriteLine(dashes);
        }

        private static void table_diff_microsoft(Schema oldschema, Schema newschema)
        {
            Console.WriteLine("\n--  Remove indexes and checks\n");
            foreach (ISchemaElement olde in oldschema)
            {
                ISchemaElement newe = newschema.find(olde.get_name());
                if (newe == null)
                {
                    if (olde is Index || olde is Check)
                        olde.drop_microsoft();
                }
                else if (newe is Index)
                    if ((Index)olde != (Index)newe)
                        olde.drop_microsoft();                
            }

            Console.WriteLine("\n--  Add new tables\n");
            foreach (ISchemaElement newe in newschema)
            {
                ISchemaElement olde = oldschema.find(newe.get_name());
                if (olde == null && newe is Table)
                    newe.add_microsoft();
            }

            Console.WriteLine("\n--  Add new columns\n");
            foreach (ISchemaElement newe in newschema)
            {
                ISchemaElement olde = oldschema.find(newe.get_name());
                if (olde == null)
                    continue;
                else if (!Object.ReferenceEquals(newe.GetType(), olde.GetType()))
                {
                    Console.Error.WriteLine(newe.get_name() + " names two different types of objects.");
                    Environment.Exit(10);
                }
                else if (newe is Table)
                    foreach (ISchemaElement newcol in ((Table)newe).get_columns())
                    {
                        ISchemaElement oldcol = ((Table)olde).find(newcol.get_name());
                        if (oldcol == null)
                        {
                            if (!(newcol is Check))
                                newcol.add_microsoft();  //  newcol is a Column
                        }
                        else if (!Object.ReferenceEquals(newcol.GetType(), oldcol.GetType()))
                        {
                            Console.Error.WriteLine(newcol.get_name() + " names two different types of objects.");
                            Environment.Exit(10);
                        }
                    }
            }

            Console.WriteLine("\n--  Change existing columns\n");
            foreach (ISchemaElement newe in newschema)
            {
                ISchemaElement olde = oldschema.find(newe.get_name());
                if (olde == null)
                    continue;
                else if (!Object.ReferenceEquals(newe.GetType(), olde.GetType()))
                {
                    Console.Error.WriteLine(newe.get_name() + " names two different types of objects.");
                    Environment.Exit(10);
                }
                else if (newe is Table)
                    foreach (ISchemaElement newcol in ((Table)newe).get_columns())
                    {
                        ISchemaElement oldcol = ((Table)olde).find(newcol.get_name());
                        if (oldcol == null)
                            continue;
                        else if (!Object.ReferenceEquals(newcol.GetType(), oldcol.GetType()))
                        {
                            Console.Error.WriteLine(newcol.get_name() + " names two different types of objects.");
                            Environment.Exit(10);
                        }
                        else if (newcol is Column)
                        {
                            if (((Column)newcol).get_default() != ((Column)oldcol).get_default())
                                Column.alter_default((Column)oldcol, (Column)newcol);
                            if ((Column)newcol != (Column)oldcol)
                            {
                                foreach (ISchemaElement index in oldschema)
                                    if (index is Index && ((Index)index).isElement(((Column)oldcol).get_name()))
                                        ((Index)index).drop_microsoft();
                                Column.alter_column((Column)oldcol, (Column)newcol);
                            }
                        }
                    }
            }

            Console.WriteLine("\n--  Remove tables\n");
            foreach (ISchemaElement olde in oldschema)
            {
                ISchemaElement newe = newschema.find(olde.get_name());
                if (newe == null && olde is Table)
                    olde.drop_microsoft();
            }

            Console.WriteLine("\n--  Drop columns\n");
            foreach (ISchemaElement olde in oldschema)
            {
                ISchemaElement newe = newschema.find(olde.get_name());
                if (newe == null)
                    continue;
                else if (!Object.ReferenceEquals(newe.GetType(), olde.GetType()))
                {
                    Console.Error.WriteLine(newe.get_name() + " names two different types of objects.");
                    Environment.Exit(10);
                }
                else if (newe is Table)
                    foreach (ISchemaElement oldcol in ((Table)olde).get_columns())
                    {
                        ISchemaElement newcol = ((Table)newe).find(oldcol.get_name());
                        if (newcol == null)
                        {
                            foreach (ISchemaElement chkcol in ((Table)olde).get_columns())
                                if (chkcol is Check && ((Check)chkcol).isElement(oldcol.get_name()))
                                    chkcol.drop_microsoft();
                            oldcol.drop_microsoft();
                        }
                    }
            }

            Console.WriteLine("\n--  Add new indexes and checks\n");
            foreach (ISchemaElement newe in newschema)
            {
                ISchemaElement olde = oldschema.find(newe.get_name());
                if (olde == null)
                {
                    if (!(newe is ForeignKey) && !(newe is Table))
                        newe.add_microsoft();
                }
                else if (!Object.ReferenceEquals(newe.GetType(), olde.GetType()))
                {
                    Console.Error.WriteLine(newe.get_name() + " names two different types of objects.");
                    Environment.Exit(10);
                }
                else if (newe is Table)
                    foreach (ISchemaElement newcol in ((Table)newe).get_columns())
                    {
                        ISchemaElement oldcol = ((Table)olde).find(newcol.get_name());
                        if (oldcol == null)
                        {
                            if (newcol is Check)
                                newcol.add_microsoft();  //  newcol is a Check
                        }
                        else if (!Object.ReferenceEquals(newcol.GetType(), oldcol.GetType()))
                        {
                            Console.Error.WriteLine(newcol.get_name() + " names two different types of objects.");
                            Environment.Exit(10);
                        }
                        else if (newcol is Check)
                        {
                            if ((Check)newcol != (Check)oldcol)
                                Check.alter_check((Check)oldcol, (Check)newcol);
                        }
                    }
                else if (newe is Index)
                    if ((Index)olde != (Index)newe)
                        newe.add_microsoft();
            }

            Console.WriteLine(dashes);
        }

        private static void generate_drop_foreign_keys_postgres(Schema schema)
        {
            foreach (ISchemaElement elm in schema)
                if (elm is ForeignKey)
                    elm.drop_postgres();
            Console.WriteLine("\n\n");
        }

        private static void generate_drop_foreign_keys_microsoft(Schema schema)
        {
            foreach (ISchemaElement elm in schema)
                if (elm is ForeignKey)
                    elm.drop_microsoft();
            Console.WriteLine("\n\n");
        }

        private static void generate_foreign_keys_postgres(Schema schema)
        {
            foreach (ISchemaElement elm in schema)
                if (elm is ForeignKey)
                    ((ForeignKey)elm).add_postgres();
        }

        private static void generate_foreign_keys_microsoft(Schema schema)
        {
            foreach (ISchemaElement elm in schema)
                if (elm is ForeignKey)
                    ((ForeignKey)elm).add_microsoft();
        }

        private static void checkFile(string file)
        {
            if (!File.Exists(file))
            {
                Console.Error.WriteLine("File " + file + " does not exist.");
                Environment.Exit(-1);
            }
        }

        private static int compare(ISchemaElement e1, ISchemaElement e2)
        {
            if (Object.ReferenceEquals(e1.GetType(), e2.GetType()))
                return e1.get_name().CompareTo(e2.get_name());
            if (e1 is Table)
                return -1;
            if (e2 is Table)
                return 1;
            if (e1 is Index)
                return -1;
            if (e2 is Index)
                return 1;
            if (e1 is ForeignKey)
                return -1;
            if (e2 is ForeignKey)
                return 1;
            return -1;
        }

        private static Schema sort_schema(Schema schema)
        {
            List<ISchemaElement> lst = new List<ISchemaElement>();
            foreach (ISchemaElement elm in schema)
                lst.Add(elm);

            lst.Sort(compare);

            Schema schema2 = new Schema(schema.get_dbtype());
            foreach (ISchemaElement elm in lst)
                schema2.AddLast(elm);
            return schema2;
        }

        private static void display_postgres_schema(Schema schema)
        {
            foreach (ISchemaElement elm in schema)
                elm.add_postgres();
        }

        private static void display_microsoft_schema(Schema schema)
        {
            foreach (ISchemaElement elm in schema)
                elm.add_microsoft();
        }

        private static void display_oracle_schema(Schema schema)
        {
            foreach (ISchemaElement elm in schema)
                elm.add_oracle();
        }

        public static string strip_quotes(string name)
        {
            if (name[0] == '"')
                return name.Remove(0, 1).Remove(name.Length - 2, 1);
            return name;
        }

        public static string quote_two_names(string name)
        {
            int i = name.IndexOf('.');
            if (i >= 0)
                return "\"" + name.Substring(0, i) + "\".\"" + name.Substring(i + 1, name.Length - i - 1) + "\"";
            else
                return "\"" + name + "\"";
        }

        public static string list_to_string(LinkedList<string> lst)
        {
            string res = "";
            if (lst == null)
                return "";
            foreach (string str in lst)
            {
                char lchar, fchar;
                if (res.Length == 0)
                    lchar = ' ';
                else
                    lchar = res[res.Length - 1];
                fchar = str[0];
                if (char.IsLetterOrDigit(lchar) && char.IsLetterOrDigit(fchar))
                    res = res + " ";
                res = res + str;
            }
            return res;
        }

        public static string limit_name(int len, string str)
        {
            if (str.Length <= len)
                return str;
            return str.Substring(0, len);
        }
    }
}
