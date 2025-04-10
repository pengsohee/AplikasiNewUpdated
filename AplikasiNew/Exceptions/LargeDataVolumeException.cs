namespace AplikasiNew.Exceptions
{
    public class LargeDataVolumeException : Exception
    {
        public LargeDataVolumeException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
