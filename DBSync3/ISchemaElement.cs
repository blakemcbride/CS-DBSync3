using System;
using System.Collections.Generic;
using System.Text;

namespace DBSync3
{
    interface ISchemaElement
    {
        void add_postgres();
        void add_microsoft();
        void add_oracle();
        string get_name();
        void drop_postgres();
        void drop_microsoft();
    }
}
