using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

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

        public void Add(string Name, string Message)
        {
            if (Errors == null)
                Errors = new List<KeyValuePair<string, string>>();

            Errors.Add(new KeyValuePair<string, string>(Name, Message));
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

        public ReturnValue(Exception ex)
        {
            var returnValue = new ReturnValue(ex);
            Success = returnValue.Success;
            Exception = returnValue.Exception;
            Message = returnValue.Message;
        }

        public ReturnValue(ReturnValue returnValue)
        {
            Success = returnValue.Success;
            Message = returnValue.Message;
            Exception = returnValue.Exception;
        }

        public ReturnValue(ReturnValue<object> returnValue)
        {
            Success = returnValue.Success;
            Message = returnValue.Message;
            Exception = returnValue.Exception;
            Value = (T)returnValue.Value;
        }

        public ReturnValue<JToken> GetJToken()
        {
            JToken jValue = null;
            if(Value != null)
            {
                jValue = JToken.FromObject(Value);
            }
            var result = new ReturnValue<JToken>(Success, Message, Exception, jValue);
            return result;
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

        public ReturnValue(Exception exception)
        {
            Success = false;
            Exception = exception;

            StringBuilder message = new StringBuilder();

            if(exception == null)
            {
                message.Append( "An error was raised with no exception.");
            }
            else
            {
                message.Append("The following error occurred: " + exception.Message);

                if(exception.InnerException != null)
                {
                    message.AppendLine("An inner exception also occurred: " + exception.InnerException.Message);
                }
            }

            Message = message.ToString();
        }

        public bool ContainsError()
        {
            return Exception != null;
        }

        public bool Success {   get; set; }
        public string Message { get; set; }

        [JsonIgnore]
        public Exception Exception { get; set; }

        public string ExceptionDetails
        {
            get
            {
                if (Exception == null)
                {
                    return "";
                }
                else
                {
                    var properties = Exception.GetType().GetProperties();
                    var fields = properties
                                     .Select(property => new {
                                         Name = property.Name,
                                         Value = property.GetValue(Exception, null)
                                     })
                                     .Select(x => String.Format(
                                         "{0} = {1}",
                                         x.Name,
                                         x.Value != null ? x.Value.ToString() : String.Empty
                                     ));
                    return String.Join("\n", fields);
                }
            }
        }
    }


}
