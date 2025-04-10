namespace AplikasiNew.Exceptions
{
    public class InvalidConnectionStringException : Exception
    {
        public InvalidConnectionStringException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
