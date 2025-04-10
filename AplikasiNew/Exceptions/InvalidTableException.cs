namespace AplikasiNew.Exceptions
{
    public class InvalidTableException : Exception
    {
        public InvalidTableException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
