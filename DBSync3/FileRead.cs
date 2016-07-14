using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace DBSync3
{
    class FileRead
    {
        private FileStream port;
        private LinkedList<char> push_list;
        private bool eof;

        public FileRead(string file)
        {
            port = new FileStream(file, FileMode.Open);
            push_list = new LinkedList<char>();
            eof = false;
        }

        public char readc()
        {
            if (push_list.First != null)
            {
                char c = push_list.First.Value;
                push_list.RemoveFirst();
                return c;
            }
            else if (eof)
                return (char)0;
            else
            {
                int c = port.ReadByte();
                if (c == -1)
                {
                    eof = true;
                    port.Close();
                    port = null;
                    return (char)0;
                }
                return (char)c;
            }
        }

        public char pushc(char c)
        {
            push_list.AddFirst(c);
            return c;
        }

        public char peekc()
        {
            char c = this.readc();
            if (c == (char)0)
                return c;
            else
                push_list.AddFirst(c);
            return c;
        }

        public void Close()
        {
            if (port != null)
            {
                port.Close();
                port = null;
            }
        }

    }
}
