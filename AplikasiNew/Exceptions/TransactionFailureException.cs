namespace AplikasiNew.Exceptions
{
    public class TransactionFailureException : Exception
    {
        public TransactionFailureException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
