namespace AplikasiNew.Exceptions
{
    public class DatabaseNetworkException : Exception
    {
        public DatabaseNetworkException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
