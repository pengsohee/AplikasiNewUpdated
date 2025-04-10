namespace AplikasiNew.Exceptions
{
    public class DataIntegrityViolationException : Exception
    {
        public DataIntegrityViolationException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
