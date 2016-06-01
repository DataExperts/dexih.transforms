using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;

namespace dexih.functions
{
    public class ReturnValueErrors<T>: ReturnValue
    {
        public ReturnValueErrors(bool success, T value, List<KeyValuePair<string, string>> errors)
        {
            Success = success;
            Errors = errors;
            Value = value;
        }

        public ReturnValueErrors(bool success, string message, Exception exception)
        {
            Success = success;
            Message = message;
            Exception = exception;
        }

        public ReturnValueErrors(ReturnValue returnValue)
        {
            Success = returnValue.Success;
            Message = returnValue.Message;
            Exception = returnValue.Exception;
        }

        public List<KeyValuePair<string, string>> Errors { get; set; }
        public T Value { get; set; }

    }

    public class ReturnValue<T> : ReturnValue
    {
        public ReturnValue(){}

        public ReturnValue(bool success, string message, Exception exception, T value)
        {
            Success = success;
            Message = message;
            Exception = exception;
            Value = value;
        }

        public ReturnValue(bool success, string message, Exception exception)
        {
            Success = success;
            Message = message;
            Exception = exception;
        }

        public ReturnValue(bool success, T value)
        {
            Success = success;
            Value = value;
            Message = "";
        }

        public ReturnValue(ReturnValue returnValue)
        {
            Success = returnValue.Success;
            Message = returnValue.Message;
            Exception = returnValue.Exception;
        }

        public T Value { get; set; }

    }

    public class ReturnValue
    {
        public ReturnValue()
        {
        }
        public ReturnValue(bool success)
        {
            Success = success;
            Message = "";
        }
        public ReturnValue(bool success, string message, Exception exception)
        {
            Success = success;
            Message = message;
            Exception = exception;
        }

        public bool ContainsError()
        {
            return Exception != null;
        }

        public bool Success { get; set; }
        public string Message { get; set; }

        [JsonIgnore]
        public Exception Exception { get; set; }
    }

    public class Message : ReturnValue<string>
    {
        public string MessageToken { get; set; }
        public string RemoteToken { get; set; }
        public string Command { get; set; }
        public int SubscriptionKey { get; set; }

        public Message() { }

        public Message(string remoteToken, string messageToken, string command, ReturnValue<string> returnValue)
        {
            RemoteToken = remoteToken;
            MessageToken = messageToken;
            Command = command;
            Success = returnValue.Success;
            Message = returnValue.Message;
            Exception = returnValue.Exception;
            if (returnValue.Value == null)
                Value = "";
            else
                Value = JsonConvert.SerializeObject(returnValue.Value);
        }

        public Message(string remoteToken, string messageToken, string command, object returnValue)
        {
            RemoteToken = remoteToken;
            MessageToken = messageToken;
            Command = command;
            Success = true;
            Message = "";
            Exception = null;
            if (returnValue == null)
                Value = "";
            else
                Value = JsonConvert.SerializeObject(returnValue);
        }

        public Message(string remoteToken, string messageToken, string command, string returnValue)
        {
            RemoteToken = remoteToken;
            MessageToken = messageToken;
            Command = command;
            Success = true;
            Message = "";
            Exception = null;
            Value = returnValue;
        }

        public Message(bool success, string message, Exception exception)
        {
            Success = success;
            Message = message;
            Exception = exception;
        }

    }
}
