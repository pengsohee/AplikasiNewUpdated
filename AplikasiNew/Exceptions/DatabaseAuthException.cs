namespace AplikasiNew.Exceptions
{
    public class DatabaseAuthException : Exception
    {
        public DatabaseAuthException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
