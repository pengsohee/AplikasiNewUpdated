using System.Xml.Xsl;

namespace AplikasiNew.Exceptions
{
    public class SchemaMismatchException : Exception
    {
        public SchemaMismatchException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
