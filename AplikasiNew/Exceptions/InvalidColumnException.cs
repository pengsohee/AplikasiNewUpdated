namespace AplikasiNew.Exceptions
{
    public class InvalidColumnException : Exception
    {
        public InvalidColumnException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
