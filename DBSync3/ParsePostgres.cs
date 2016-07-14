using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace DBSync3
{
    class ParsePostgres
    {
        public static Schema parse_postgres_schema(string file)
        {
            Schema schema = new Schema(Schema.Postgres);
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
                    ISchemaElement res = parse_create(fp);
                    if (res != null)
                        schema.AddLast(res);
                }
                else if (word == "ALTER")
                {
                    ISchemaElement res = parse_alter(fp);
                    if (res != null)
                        schema.AddLast(res);
                }
                else if (word == "REVOKE")
                    parse_revoke(fp);
                else if (word == "GRANT")
                    parse_grant(fp);
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

        private static ISchemaElement parse_create(FileRead fp)
        {
            string word = get_word(fp);
            if (word == "")
                return null;
            if (word == "TABLE")
            {
                string tname = get_word(fp);
                Table table = new Table(tname);
                get_word(fp);  //  eat "("
                while (true)
                {
                    word = get_word(fp);
                    if (word == "")
                        return table;
                    if (word == ")")
                    {
                        get_word(fp);  //  eat final ";"
                        return table;
                    }
                    if (word == "CONSTRAINT")
                    {
                        Check con = parse_constraint(fp, tname);
                        table.add_column(con);
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
                tname = get_word(fp);
                word = get_word(fp); // "USING"
                word = get_word(fp); // "btree"
                word = get_word(fp); // "("
                fields = get_field_list(fp);
                word = get_word(fp); // ";"
                return new Index(iname, tname, false, "INDEX", true, fields);
            }
            else if (word == "UNIQUE")
            {
                string iname, tname;
                LinkedList<string> fields;
                word = get_word(fp);  // "INDEX"
                iname = get_word(fp);
                word = get_word(fp);  // "ON"
                tname = get_word(fp);
                word = get_word(fp); // "USING"
                word = get_word(fp); // "btree"
                word = get_word(fp); // "("
                fields = get_field_list(fp);
                word = get_word(fp); // ";"
                return new Index(iname, tname, false, "UNIQUE", true, fields);
            }
            else
                return eat_statement(fp);
        }

        private static Column parse_column(FileRead fp, string word, string tname)
        {
            Column col = new Column(Schema.Postgres, word, tname);
            word = get_word(fp);  //  first word in type
            if (word == "character")
                return parse_column_character(fp, col, word);
            else if (word == "integer" ||
                word == "smallint" ||
                word == "real" ||
                word == "double")
                return parse_column_number(fp, col, word);
            else if (word == "timestamp" ||
                word == "date" ||
                word == "time")
                return parse_column_timestamp(fp, col, word);
            else if (word == "text")
                return parse_column_text(fp, col, word);
            else if (word == "bytea")
                return parse_column_bytea(fp, col, word);
            else
                return col;
        }

        private static Column parse_column_character(FileRead fp, Column col, string word)
        {
            LinkedList<string> lst = new LinkedList<string>();
            lst.AddLast(word); // "character"
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
            if (word == "DEFAULT")
            {
                word = get_word(fp);  //  the whole string
                //				word = word + get_word(fp);  // the default
                //				word = word + get_word(fp); //  the "'"
                col.set_default(word);
                word = get_word(fp);
                if (word == ":")
                {
                    get_word(fp);  //  eat second ":"
                    word = get_word(fp);  //  eat the type
		    if (word == "double")
		    	get_word(fp);     //  eat "precision"
                    word = get_word(fp);
	    	    if (word == "varying")
	    		word = get_word(fp);  //  skip varying
                }
            }
            if (word == "NOT")
            {
                get_word(fp);  // the "NULL"
                col.set_not_null(true);
                word = get_word(fp);
            }
            if (word == ")")
                fp.pushc(')');
            return col;
        }

        private static Column parse_column_number(FileRead fp, Column col, string word)
        {
            LinkedList<string> lst = new LinkedList<string>();
            lst.AddLast(word); // "integer" or "smallint", etc.
            if (word == "double")
                lst.AddLast(get_word(fp));  //  the "precision"
            col.set_type(lst);
            word = get_word(fp);
            if (word == "DEFAULT")
            {
	    	word = get_word(fp);
		if (word == "(")
		    word = get_word(fp);
                col.set_default(word);
                word = get_word(fp);
		if (word == ")")
		    word = get_word(fp);
            }
            if (word == "NOT")
            {
                get_word(fp);  //  the "NULL"
                col.set_not_null(true);
                word = get_word(fp);
            }
            if (word == ")")
                fp.pushc(')');
            return col;
        }

        private static Column parse_column_timestamp(FileRead fp, Column col, string word)
        {
            LinkedList<string> lst = new LinkedList<string>();
            lst.AddLast(word); // "timestamp"
            word = get_word(fp);
            if (word == "without" || word == "with")
            {
                lst.AddLast(word);
                lst.AddLast(get_word(fp)); // "time"
                lst.AddLast(get_word(fp)); // "zone"
                word = get_word(fp);
            }
            col.set_type(lst);
            if (word == "DEFAULT")
            {
                col.set_default(get_word(fp));
                word = get_word(fp);
            }
            if (word == "NOT")
            {
                get_word(fp); // the "NULL"
                col.set_not_null(true);
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
            if (word == "NOT")
            {
                get_word(fp); // the "NULL"
                col.set_not_null(true);
                word = get_word(fp);
            }
            if (word == ")")
                fp.pushc(')');
            return col;
        }

        private static Column parse_column_bytea(FileRead fp, Column col, string word)
        {
            LinkedList<string> lst = new LinkedList<string>();
            lst.AddLast(word); // "bytea"
            col.set_type(lst);
            word = get_word(fp);
            if (word == "DEFAULT")
            {
                col.set_default(get_word(fp));
                word = get_word(fp);
            }
            if (word == "NOT")
            {
                get_word(fp);  //  the "NULL"
                col.set_not_null(true);
                word = get_word(fp);
            }
            if (word == ")")
                fp.pushc(')');
            return col;
        }

        private static Check parse_constraint(FileRead fp, string tname)
        {
            string word = get_word(fp);
            Check con = new Check(Schema.Postgres, word, tname);
            LinkedList<string> lst = new LinkedList<string>();
            int n = 0;  //  number of left parens

            get_word(fp);  //  "CHECK"
            word = get_word(fp); //  should be "("
            if (word == "(")
                n++;
            lst.AddLast(word);
            word = get_word(fp);
            while (true)
            {
                if (word == "(")
                    n++;
                else if (word == ")")
                    n--;
                if (word == ":")
                {
                    get_word(fp);  //  should be the next ":"
                    word = get_word(fp);  //  should be the type
		    if (word == "double")
		    	get_word(fp);     //  should be "precision"
		    word = get_word(fp);
		    if (word == "varying")   //  a "character varying" type
		    	word = get_word(fp);
                }
                else
                {
                    lst.AddLast(word);
                    if (n == 0)
                        break;
		    word = get_word(fp);
                }
            }
            con.set_constraint(lst);
            word = get_word(fp);  // either "," or ")"
            if (word == ")")
                fp.pushc(')');
            return con;
        }

        private static ISchemaElement parse_alter(FileRead fp)
        {
            string word = get_word(fp);  // "TABLE"
            bool only = false;
            string tname = "";
            string cname = "";
            LinkedList<string> fields;
            LinkedList<string> fields2;
            string tname2 = "";

            word = get_word(fp);
            if (word == "ONLY")
            {
                only = true;
                word = get_word(fp);
            }
            tname = word;  //  table name
            word = get_word(fp);  //  either "ADD" or "OWNER"
            if (word == "ADD")
            {
                word = get_word(fp);  //  "CONSTRAINT"
                cname = get_word(fp);  //  constraint name
                word = get_word(fp);   //  "PRIMARY", "UNIQUE" or "FOREIGN"
                if (word == "PRIMARY")
                {
                    word = get_word(fp);  //  "KEY"
                    word = get_word(fp); //  "("
                    fields = get_field_list(fp);
                    word = get_word(fp);  //  ";"
                    return new Index(cname, tname, only, "PRIMARY KEY", false, fields);
                }
                else if (word == "UNIQUE")
                {
                    word = get_word(fp);  // "("
                    fields = get_field_list(fp);
                    word = get_word(fp);  //  ";"
                    return new Index(cname, tname, only, "UNIQUE", false, fields);
                }
                else if (word == "FOREIGN")
                {
                    word = get_word(fp);  //  "KEY"
                    word = get_word(fp);  //  "("
                    fields = get_field_list(fp);
                    word = get_word(fp);  //  "REFERENCES"
                    tname2 = get_word(fp);
                    word = get_word(fp);  //  "("
                    fields2 = get_field_list(fp);
                    word = get_word(fp);  //  ";"
                    return new ForeignKey(cname, tname, only, fields, tname2, fields2);
                }
                else
                    return eat_statement(fp);
            }
            else
                return eat_statement(fp);
        }

        private static LinkedList<string> get_field_list(FileRead fp)
        {
            LinkedList<string> lst = new LinkedList<string>();
            string word, tmp;

	    word = get_word(fp);
            while (true)
            {
                if (word == ")" || word == "")
                    return lst;
                if (word != ",")
                    if (word == "upper"  ||  word == "lower")
                    {
                        word += get_word(fp);  //  "("
                        tmp = get_word(fp);  //  "(" or name
			if (tmp == "(")
                            word += get_word(fp);  //  column name
			else
			    word += tmp;
                        word += get_word(fp);  //  ")"
                        tmp = get_word(fp);  //  ":" or no type
			if (tmp == ":") {
                            get_word(fp);  //  ":"
                            tmp = get_word(fp);   //  the type
			    if (tmp == "double")
				get_word(fp);  //  "precision"
			    tmp = get_word(fp);    //  "varying" or ")"
			    if (tmp == "varying")
                        	get_word(fp);  //  ")"
                            lst.AddLast(word);
			    word = get_word(fp);
			} else {
			    lst.AddLast(word);
			    word = tmp;
			}
                    }
                    else
		    {
                        lst.AddLast(word);
			word = get_word(fp);
		    }
		else
		    word = get_word(fp);
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
            } while (word != "" && word != ";");
            return null;
        }

        private static string get_word(FileRead fp)
        {
            return get_word((char)0, fp);
        }

        private static string get_word(char c, FileRead fp)
        {
            if (c == '"')
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
                if (c == '"')
                    continue;  //  skip "
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
    }
}
