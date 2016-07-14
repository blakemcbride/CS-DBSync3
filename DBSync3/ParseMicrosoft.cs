using System;
using System.Collections.Generic;
using System.Text;

namespace DBSync3
{
    class ParseMicrosoft
    {
        public static Schema parse_microsoft_schema(string file)
        {
            Schema schema = new Schema(Schema.Microsoft);
            FileRead fp = new FileRead(file);
            char c, nc;
            while ((char)0 != (c = fp.readc()))
            {
                if (is_space(c))
                    continue;
                nc = fp.peekc();
                if (c == '-' && nc == '-')
                {
                    skip_to_eol(fp);
                    continue;
                }
                if (c == '/' && nc == '*')
                {
                    skip_comment(fp);
                    continue;
                }
                string word = get_word(c, fp);
                if (word == "SET")
                    parse_set(fp);
                else if (word == "COMMENT")
                {
                    ISchemaElement res = parse_comment(fp);
                    if (res != null)
                        schema.AddLast(res);
                }
                else if (word == "CREATE")
                {
                    ISchemaElement res = parse_create(fp, schema);
                    if (res != null)
                        schema.AddLast(res);
                }
                else if (word == "ALTER")
                {
                    parse_alter(fp, schema);
                }
                else if (word == "REVOKE")
                    parse_revoke(fp);
                else if (word == "GRANT")
                    parse_grant(fp);
                else if (word == "USE")
                    parse_use(fp);
                else
                    Console.Error.WriteLine("Unexpected keyword " + word);
            }
            fp.Close();
            return schema;
        }

        private static void parse_set(FileRead fp)
        {
            eat_statement(fp);
        }

        private static void parse_use(FileRead fp)
        {
            eat_statement(fp);
        }

        private static ISchemaElement parse_comment(FileRead fp)
        {
            string word, type, what, on = "";
            get_word(fp); // "ON"
            type = get_word(fp);
            what = get_word(fp);
            if (type == "CONSTRAINT")
            {
                get_word(fp); // "ON"
                on = get_word(fp);
                get_word(fp);  //  "IS"
                word = get_word(fp);  //  comment
                get_word(fp);  //  ";"
                return new Comment(type, what, on, word);
            }
            else
            {
                get_word(fp);  // "IS"
                word = get_word(fp);  // comment
                get_word(fp); // ";"
                return new Comment(type, what, on, word);
            }
        }

        private static ISchemaElement parse_create(FileRead fp, Schema schema)
        {
            string word = get_word(fp);
            if (word == "")
                return null;
            if (word == "TABLE")
            {
                string tname = drop_dbo(get_word(fp));
                Table table = new Table(tname);
                get_word(fp);  //  eat "("
                while (true)
                {
                    word = get_word(fp);
                    if (word == "")
                        return table;
                    if (word == ")")
                    {
                        eat_statement(fp);  //  eat till the "GO"
                        return table;
                    }
                    if (word == "CONSTRAINT")
                    {
                        parse_constraint(fp, tname, table, schema);
                    }
                    else
                    {
                        Column col = parse_column(fp, word, tname);
                        table.add_column(col);
                    }
                }
            }
            else if (word == "INDEX")
            {
                string iname, tname;
                LinkedList<string> fields;
                iname = get_word(fp);
                word = get_word(fp);  // "ON"
                tname = drop_dbo(get_word(fp));
                word = get_word(fp); // "("
                fields = get_field_list(fp);
                word = get_word(fp);
                if (word == "WITH")
                {
                    word = get_word(fp);
                    if ("FILLFACTOR" != word)  // eat "FILLFACTOR"
                    {
                        Console.Error.WriteLine("Expected \"FILLFACTOR\" but got " + word);
                        Environment.Exit(11);
                    }
                    get_word(fp);  //  eat "="
                    get_word(fp);  //  eat number
                    word = get_word(fp);
                }
                if (word == "ON")
                {
                    get_word(fp);  //  eat "PRIMARY"
                    word = get_word(fp);  //  "GO"?
                    if (word == "TEXTIMAGE_ON")
                    {
                        word = get_word(fp);  //  eat "PRIMARY"
                        word = get_word(fp);  //  "GO"
                    }
                }
                return new Index(iname, tname, false, "INDEX", true, fields);
            }
            else if (word == "UNIQUE")
            {
                string iname, tname;
                LinkedList<string> fields;
                word = get_word(fp);  // "INDEX"
                iname = get_word(fp);
                word = get_word(fp);  // "ON"
                tname = drop_dbo(get_word(fp));
                word = get_word(fp); // "("
                fields = get_field_list(fp);
                word = get_word(fp);
                if (word == "WITH")
                {
                    word = get_word(fp);
                    if ("FILLFACTOR" != word)  // eat "FILLFACTOR"
                    {
                        Console.Error.WriteLine("Expected \"FILLFACTOR\" but got " + word);
                        Environment.Exit(11);
                    }
                    get_word(fp);  //  eat "="
                    get_word(fp);  //  eat number
                    word = get_word(fp);
                }
                if (word == "ON")
                {
                    get_word(fp);  //  eat "PRIMARY"
                    word = get_word(fp);  //  "GO"?
                    if (word == "TEXTIMAGE_ON")
                    {
                        word = get_word(fp);  //  eat "PRIMARY"
                        word = get_word(fp);  //  "GO"
                    }
                }
                return new Index(iname, tname, false, "UNIQUE", true, fields);
            }
            else
                return eat_statement(fp);
        }

        private static string drop_dbo(string x)
        {
            if (x.Length > 4 && x.StartsWith("dbo."))
                return x.Remove(0, 4);
            else
                return x;
        }

        private static Column parse_column(FileRead fp, string word, string tname)
        {
            Column col = new Column(Schema.Microsoft, word, tname);
            word = get_word(fp);  //  first word in type
            if (word == "char" || word == "varchar" || word == "character")
                return parse_column_character(fp, col, word);
            else if (word == "int" ||
                word == "integer" ||
                word == "smallint" ||
                word == "float" ||
                word == "real" ||
                word == "double")
                return parse_column_number(fp, col, word);
            else if (word == "timestamp" ||
                word == "datetime" ||
                word == "date" ||
                word == "time")
                return parse_column_timestamp(fp, col, word);
            else if (word == "text")
                return parse_column_text(fp, col, word);
            else if (word == "image")
                return parse_column_image(fp, col, word);
            else
                return col;
        }

        private static Column parse_column_character(FileRead fp, Column col, string word)
        {
            LinkedList<string> lst = new LinkedList<string>();
            if (word == "varchar")
            {
                lst.AddLast("character");
                lst.AddLast("varying");
            }
            else if (word == "char")
                lst.AddLast("character");
            else
                lst.AddLast(word);
            word = get_word(fp);
            if (word == "varying")
            {
                lst.AddLast(word);
                word = get_word(fp);  //  expecting "("
            }
            lst.AddLast(word);  //  the "("
            lst.AddLast(get_word(fp)); //  the number in the parenthesis
            lst.AddLast(get_word(fp)); // the ")"
            col.set_type(lst);


            word = get_word(fp);
            while (word != ")" && word != ",")
            {
                if (word == "COLLATE")
                {
                    get_word(fp);  //  the colating sequence
                    word = get_word(fp);
                }
                else if (word == "CONSTRAINT")
                {
                    get_word(fp);  //  eat constraint name
                    word = get_word(fp);
                }
                else if (word == "DEFAULT")
                {
                    int parens = 0;
                    word = get_word(fp);
                    while (word == "(")
                    {
                        word = get_word(fp);
                        parens++;
                    }
                    col.set_default(word);
                    while (parens-- != 0)
                        word = get_word(fp);  //  ")"
                    word = get_word(fp);
                }
                else if (word == "NOT")
                {
                    get_word(fp);  // the "NULL"
                    col.set_not_null(true);
                    word = get_word(fp);
                }
                else if (word == "NULL")
                    word = get_word(fp);
            }


            if (word == ")")
                fp.pushc(')');
            return col;
        }

        private static Column parse_column_number(FileRead fp, Column col, string word)
        {
            LinkedList<string> lst = new LinkedList<string>();
            if (word == "int")
                lst.AddLast("integer");
            else if (word == "float")
            {
                lst.AddLast("double");
                lst.AddLast("precision");
            }
            else
                lst.AddLast(word);
            if (word == "double")
                lst.AddLast(get_word(fp));  //  the "precision"
            col.set_type(lst);


            word = get_word(fp);
            while (word != ")" && word != ",")
            {
                if (word == "CONSTRAINT")
                {
                    get_word(fp);  //  eat constraint name
                    word = get_word(fp);
                } else if (word == "DEFAULT")
                {
                    int parens = 0;
                    word = get_word(fp);
                    while (word == "(")
                    {
                        word = get_word(fp);
                        parens++;
                    }
                    col.set_default(word);
                    while (parens-- != 0)
                        word = get_word(fp);  //  ")"
                    word = get_word(fp);
                }
                else if (word == "NOT")
                {
                    get_word(fp);  //  the "NULL"
                    col.set_not_null(true);
                    word = get_word(fp);
                } else if (word == "NULL")
                    word = get_word(fp);
            }


            if (word == ")")
                fp.pushc(')');
            return col;
        }

        private static Column parse_column_timestamp(FileRead fp, Column col, string word)
        {
            LinkedList<string> lst = new LinkedList<string>();
            lst.AddLast("timestamp"); // "timestamp"
            word = get_word(fp);
            if (word == "without")
            {
                lst.AddLast(word);
                lst.AddLast(get_word(fp)); // "time"
                lst.AddLast(get_word(fp)); // "zone"
                word = get_word(fp);
            }
            col.set_type(lst);

            while (word != ")" && word != ",")
            {
                if (word == "CONSTRAINT")
                {
                    get_word(fp);  //  eat constraint name
                    word = get_word(fp);
                }
                else if (word == "DEFAULT")
                {
                    int parens = 0;
                    word = get_word(fp);
                    while (word == "(")
                    {
                        word = get_word(fp);
                        parens++;
                    }
                    col.set_default(word);
                    while (parens-- != 0)
                        word = get_word(fp);  //  ")"
                    word = get_word(fp);
                }
                else if (word == "NOT")
                {
                    get_word(fp); // the "NULL"
                    col.set_not_null(true);
                    word = get_word(fp);
                }
                else if (word == "NULL")
                    word = get_word(fp);
            }


            if (word == ")")
                fp.pushc(')');
            return col;
        }

        private static Column parse_column_text(FileRead fp, Column col, string word)
        {
            LinkedList<string> lst = new LinkedList<string>();
            lst.AddLast(word); // "text"
            col.set_type(lst);

            word = get_word(fp);
            while (word != ")" && word != ",")
            {
                if (word == "COLLATE")
                {
                    get_word(fp);  //  the collating sequence
                    word = get_word(fp);
                }
                else if (word == "NOT")
                {
                    get_word(fp); // the "NULL"
                    col.set_not_null(true);
                    word = get_word(fp);
                }
                else if (word == "NULL")
                    word = get_word(fp);
            }


            if (word == ")")
                fp.pushc(')');
            return col;
        }

        private static Column parse_column_image(FileRead fp, Column col, string word)
        {
            LinkedList<string> lst = new LinkedList<string>();
            lst.AddLast("bytea");
            col.set_type(lst);


            word = get_word(fp);
            while (word != ")" && word != ",")
            {
                if (word == "CONSTRAINT")
                {
                    get_word(fp);  //  eat constraint name
                    word = get_word(fp);
                }
                else if (word == "DEFAULT")
                {
                    col.set_default(get_word(fp));
                    word = get_word(fp);
                }
                else if (word == "NOT")
                {
                    get_word(fp);  //  the "NULL"
                    col.set_not_null(true);
                    word = get_word(fp);
                }
                else if (word == "NULL")
                    word = get_word(fp);
            }


            if (word == ")")
                fp.pushc(')');
            return col;
        }


        //  This is a constraint inside a create table

        private static void parse_constraint(FileRead fp, string tname, Table table, Schema schema)
        {
            string cname = get_word(fp);
            string word = get_word(fp);

            if (word == "CHECK")
            {
                Check con = new Check(Schema.Microsoft, cname, tname);
                LinkedList<string> lst = new LinkedList<string>();
                int n = 0;  //  number of left parens

                word = get_word(fp); //  should be "("
                if (word == "(")
                    n++;
                lst.AddLast(word);
                while (true)
                {
                    word = get_word(fp);
                    if (word == "(")
                        n++;
                    else if (word == ")")
                        n--;
                    if (word == ":")
                    {
                        get_word(fp);  //  should be the next ":"
                        get_word(fp);  //  should be the "bpchar"
                    }
                    else
                    {
                        lst.AddLast(word);
                        if (n == 0)
                            break;
                    }
                }
                con.set_constraint(lst);
                word = get_word(fp);  // either "," or ")"
                if (word == ")")
                    fp.pushc(')');
                table.add_column(con);
            }
            else if (word == "PRIMARY")
            {
                parse_primary_key(fp, cname, tname, schema);
            }
            else if (word == "UNIQUE")
            {
                parse_unique(fp, cname, tname, schema);
            }
            else
            {
                Console.Error.WriteLine("Expected CONSTRAINT type in a CREATE TABLE - " + word);
                Environment.Exit(11);
            }
        }

        private static string parse_primary_key(FileRead fp, string cname, string tname, Schema schema)
        {
            LinkedList<string> fields;
            bool only = false;
            string word = get_word(fp);  //  KEY
            word = get_word(fp);
            if (word == "CLUSTERED" || word == "NONCLUSTERED")
                word = get_word(fp);
            //  word expected to be "(" at this point
            fields = get_field_list(fp);
            word = get_word(fp);
            if (word == "WITH")
            {
                word = get_word(fp);
                if (word == "(")
                {
                    get_field_list(fp);  //  eat it
                }
                else
                {
                    if ("FILLFACTOR" != word)  // eat "FILLFACTOR"
                    {
                        Console.Error.WriteLine("Expected \"FILLFACTOR\" but got " + word);
                        Environment.Exit(11);
                    }
                    get_word(fp);  //  eat "="
                    get_word(fp);  //  eat number    
                }
                word = get_word(fp);
            }
            if (word == "ON")
            {
                get_word(fp);  // eat "PRIMARY"
                word = get_word(fp);
                if (word == "TEXTIMAGE_ON")
                {
                    word = get_word(fp);  //  eat "PRIMARY"
                    word = get_word(fp);
                }
            }
            if (word == ")")
                fp.pushc(')');
            schema.AddLast(new Index(cname, tname, only, "PRIMARY KEY", false, fields));
	    return word;
        }

        private static string parse_unique(FileRead fp, string cname, string tname, Schema schema)
        {
            LinkedList<string> fields;
            bool only = false;
            string word = get_word(fp);
            if (word == "CLUSTERED" || word == "NONCLUSTERED")
                word = get_word(fp);
            //  word expected to be "(" at this point
            fields = get_field_list(fp);
            word = get_word(fp);
            if (word == "WITH")
            {
                word = get_word(fp);
                if (word == "(")
                {
                    get_field_list(fp);  //  eat it
                }
                else
                {
                    if ("FILLFACTOR" != word)  // eat "FILLFACTOR"
                    {
                        Console.Error.WriteLine("Expected \"FILLFACTOR\" but got " + word);
                        Environment.Exit(11);
                    }
                    get_word(fp);  //  eat "="
                    get_word(fp);  //  eat number    
                }
                word = get_word(fp);
            }
            if (word == "ON")
            {
                get_word(fp);  //  eat "PRIMARY"
                word = get_word(fp);
                if (word == "TEXTIMAGE_ON")
                {
                    word = get_word(fp);  //  eat "PRIMARY"
                    word = get_word(fp);
                }
            }
            if (word == ")")
                fp.pushc(')');
            schema.AddLast(new Index(cname, tname, only, "UNIQUE", false, fields));
	    return word;
        }

        private static void parse_alter(FileRead fp, Schema schema)
        {
            string word = get_word(fp);  // "TABLE"
            bool only = false;
            string tname = "";
            string cname = "";
            LinkedList<string> fields;
            LinkedList<string> fields2;
            string tname2 = "";
            string dflt;
            int n = 0;
            LinkedList<string> lst;

            tname = drop_dbo(get_word(fp)); //  table name
            word = get_word(fp);
            if (word == "WITH")
            {
                get_word(fp);  //  "NOCHECK"
                word = get_word(fp);
            }
            else if (word == "CHECK")
            {
                word = get_word(fp);  //  assume "CONSTRAINT"
                if ("CONSTRAINT" != word)
                {
                    Console.Error.WriteLine("Expected \"CONSTRAINT\" but got " + word + " in ALTER TABLE " + tname);
                    Environment.Exit(11);
                }
                word = get_word(fp);  //  assume constraint name
                word = get_word(fp);  //  assume "GO"
                if ("GO" != word  &&  "" != word)
                {
                    Console.Error.WriteLine("Expected \"GO\" but got " + word + " in ALTER TABLE " + tname);
                    Environment.Exit(11);
                }
                return;
            }

            //  it is assumed that word now has "ADD"
            word = get_word(fp);
            while (true)
            {
                //  word = "CONSTRAINT", ",", or "GO"
                if (word == ",")
                {
                    word = get_word(fp);
                    continue;  //  get next constraint
                }
                if (word == "GO"  ||  word == "")
                    return;
                //  assume "CONSTRAINT"
                cname = get_word(fp);  //  the constraint name
                word = get_word(fp);   //  "PRIMARY", "DEFAULT", "CHECK", "UNIQUE", or "FOREIGN"
                if (word == "PRIMARY")
                {
                    word = parse_primary_key(fp, cname, tname, schema);
                }
                else if (word == "DEFAULT")
                {
                    get_word(fp);  //  eat "("
                    dflt = get_word(fp);  //  value or another "(" - used in negative numbers
                    if (dflt == "(")
                        dflt = get_word(fp);
                    word = get_word(fp);  //  ")"
                    word = get_word(fp);  //  ")" or "FOR"
                    if (word == ")")
                        word = get_word(fp);
                    //  word has "FOR"
                    word = get_word(fp);
                    Table table = schema.find_table(tname);
                    if (table != null)
                    {
                        Column col = (Column)table.find(word);
                        if ((object)col != null)
                            col.set_default(dflt, cname);
                    }
                    word = get_word(fp);
                }
                else if (word == "CHECK")
                {
                    get_word(fp);  //  eat "("
                    n = 1;
                    lst = new LinkedList<string>();
                    while (true)
                    {
                        word = get_word(fp);
                        if (word == "(")
                            n++;
                        else if (word == ")")
                            n--;
                        lst.AddLast(word);
                        if (n > 0)
                            continue;
                        else
                            break;
                    }
                    Check con = new Check(Schema.Microsoft, cname, tname);
                    Table table = schema.find_table(tname);
                    con.set_constraint(lst);
                    if (table != null)
                        table.add_column(con);
                    word = get_word(fp);
                }
                else if (word == "UNIQUE")
                {
                    word = parse_unique(fp, cname, tname, schema);
                }
                else if (word == "FOREIGN")
                {
                    get_word(fp);  //  eat "KEY"
                    get_word(fp);  //  eat "("
                    fields = get_field_list(fp);
                    get_word(fp);  //  eat "REFERENCES"
                    tname2 = drop_dbo(get_word(fp));
                    get_word(fp);  //  eat "("
                    fields2 = get_field_list(fp);
                    schema.AddLast(new ForeignKey(cname, tname, only, fields, tname2, fields2));
                    word = get_word(fp);
                }
                else
                {
                    eat_statement(fp);
                    break;
                }
            }
        }

        private static LinkedList<string> get_field_list(FileRead fp)
        {
            LinkedList<string> lst = new LinkedList<string>();
            string word;

            while (true)
            {
                word = get_word(fp);
                if (word == ")" || word == "")
                    return lst;
                if (word != ","  &&  word != "ASC")
                    lst.AddLast(word);
            }
        }

        private static void parse_revoke(FileRead fp)
        {
            eat_statement(fp);
        }

        private static void parse_grant(FileRead fp)
        {
            eat_statement(fp);
        }

        private static ISchemaElement eat_statement(FileRead fp)
        {
            string word;

            do
            {
                word = get_word(fp);
            } while (word != "" && word != "GO"  &&  word != ";");
            return null;
        }

        private static string get_word(FileRead fp)
        {
            return get_word((char)0, fp);
        }

        private static string get_word(char c, FileRead fp)
        {
            if (c == '[')
                c = (char)0;
            if (is_sep(c))
                return c.ToString();
            char quotec = c == '\'' ? c : (char)0;
            char nc;
            string word = c == (char)0 ? "" : c.ToString();
            while (true)
            {
                c = fp.readc();
                if (c == (char)0)
                    return word;
                if (c == '[' || c == ']')
                    continue;  //  skip [ and ]
                if (quotec != (char)0)
                {
                    nc = fp.peekc();
                    if (c == quotec && nc != quotec)
                        return word + c.ToString();
                    word = word + c.ToString();
                    if (c == quotec)
                    {
                        word = word + c.ToString();
                        fp.readc();  //  throw it away
                    }
                    continue;
                }
                if (c == '\'')
                    if (word == "")
                    {
                        word = c.ToString();
                        quotec = c;
                        continue;
                    }
                    else
                    {
                        fp.pushc(c);
                        return word;
                    }
                if (is_sep(c))
                    if (word == "")
                        return c.ToString();
                    else
                    {
                        fp.pushc(c);
                        return word;
                    }
                if (is_space(c))
                    if (word == "")
                        continue;
                    else
                        return word;
                word = word + c.ToString();
            }
        }

        private static bool is_sep(char c)
        {
            return c == '(' ||
                c == ')' ||
                c == ';' ||
                c == ',' ||
                c == ':' ||
                c == '=' ||
                c == '!' ||
                c == '<' ||
                c == '>';
        }

        private static bool is_eol(char c)
        {
            return c == '\r' || c == '\n';
        }

        private static bool is_space(char c)
        {
            return c == ' ' || c == '\t' || is_eol(c);
        }

        private static void skip_to_eol(FileRead fp)
        {
            char c;
            while ((char)0 != (c = fp.readc()))
                if (is_eol(c))
                    break;
        }

        private static void skip_comment(FileRead fp)
        {
            char c, pc=' ';
            while ((char)0 != (c = fp.readc()))
            {
                if (c == '/' && pc == '*')
                    break;
                pc = c;
            }
        }

    }
}
