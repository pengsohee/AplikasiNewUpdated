namespace AplikasiNew.Exceptions
{
    public class KeyManagementException : Exception
    {
        public KeyManagementException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
