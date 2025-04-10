namespace AplikasiNew.Exceptions
{
    public class AlgorithmIncapibilitiesException : Exception
    {
        public AlgorithmIncapibilitiesException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
