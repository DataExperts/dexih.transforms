using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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

        public void Add(string name, string message)
        {
            if (Errors == null)
                Errors = new List<KeyValuePair<string, string>>();

            Errors.Add(new KeyValuePair<string, string>(name, message));
        }

        public List<KeyValuePair<string, string>> Errors { get; set; }
        public T Value { get; set; }

    }

    public class ReturnValueMultiple<T>: ReturnValue<T[]>
    {
        public List<ReturnValue<T>> ReturnValues = new List<ReturnValue<T>>();

        public void Add(ReturnValue<T> returnValue)
        {
            ReturnValues.Add(returnValue);
        }

        public override bool Success {
            get
            {
                // if no returnValues return false
                if (ReturnValues.Count == 0) return false;

                // if any returnValue contains false, return false,
                return !ReturnValues.Exists(c => c.Success == false);
            }
        }

        public override string Message
        {
            get
            {
                var message = new StringBuilder();

                message.AppendLine($"{ReturnValues.Count(c => c.Success)} successful, {ReturnValues.Count(c => !c.Success)} failed.");

                foreach(var returnValue in ReturnValues.Where(c => !c.Success))
                {
                    message.AppendLine($"Message: " + returnValue.Message);
                }

                return message.ToString();
            }
        }

        public override string ExceptionDetails
        {
            get
            {
                var exceptionDetails = new StringBuilder();
                foreach(var returnValue in ReturnValues.Where(c => c.Exception != null))
                {
                    exceptionDetails.AppendLine("Exception Detials: " + returnValue.ExceptionDetails);
                }

                return exceptionDetails.ToString();
            }
        }

        public override T[] Value
        {
            get
            {
                return ReturnValues.Select(c => c.Value).ToArray();
            }
        }
    }

    public class ReturnValueMultiple : ReturnValue
    {
        public List<ReturnValue> ReturnValues = new List<ReturnValue>();

        public void Add(ReturnValue returnValue)
        {
            ReturnValues.Add(returnValue);
        }

        public override bool Success
        {
            get
            {
                // if no returnValues return false
                if (ReturnValues.Count == 0) return false;

                // if any returnValue contains false, return false,
                return !ReturnValues.Exists(c => c.Success == false);
            }
        }

        public override string Message
        {
            get
            {
                var message = new StringBuilder();

                message.AppendLine($"{ReturnValues.Count(c => c.Success)} successful, {ReturnValues.Count(c => !c.Success)} failed.");

                foreach (var returnValue in ReturnValues.Where(c => !c.Success))
                {
                    message.AppendLine($"Message: " + returnValue.Message);
                }

                return message.ToString();
            }
        }

        public override string ExceptionDetails
        {
            get
            {
                var exceptionDetails = new StringBuilder();
                foreach (var returnValue in ReturnValues.Where(c => c.Exception != null))
                {
					if (!string.IsNullOrEmpty(returnValue.ExceptionDetails))
					{
						exceptionDetails.AppendLine("Exception Detials: " + returnValue.ExceptionDetails);
					}
                }

                return exceptionDetails.ToString();
            }
        }

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

        private void SetReturnValue(ReturnValue returnValue)
        {
            Success = returnValue.Success;
            Message = returnValue.Message;
            Exception = returnValue.Exception;
            if (returnValue.Exception == null)
            {
                ExceptionDetails = returnValue.ExceptionDetails;
            }
        }

        public ReturnValue(ReturnValue returnValue)
        {
            SetReturnValue(returnValue);
        }

        public ReturnValue(ReturnValue<object> returnValue)
        {
            SetReturnValue(returnValue);
            Value = (T)returnValue.Value;
        }

        public ReturnValue<JToken> GetJToken()
        {
            JToken jValue = null;
            if(Value != null)
            {
				jValue = Json.JTokenFromObject(Value, ""); // JToken.FromObject(Value);
            }
            var result = new ReturnValue<JToken>(Success, Message, Exception, jValue);
            return result;
        }


        public virtual T Value { get; set; }

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

            var message = new StringBuilder();

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

        public virtual bool Success {   get; set; }
        public virtual string Message { get; set; }

        [JsonIgnore]
        public Exception Exception { get; set; }

        private string _exceptionDetails {get;set;} = "";
        public virtual string ExceptionDetails
        {
            set { _exceptionDetails = value; }
            get
            {
            
                if (Exception == null)
                {
                    if(string.IsNullOrEmpty(_exceptionDetails))
					{
						return null;
					}
                    return _exceptionDetails;
                }
                var properties = Exception.GetType().GetProperties();
                var fields = properties
                    .Select(property => new {
                        property.Name,
                        Value = property.GetValue(Exception, null)
                    })
                    .Select(x => string.Format(
                        "{0} = {1}",
                        x.Name,
                        x.Value != null ? x.Value.ToString() : string.Empty
                    ));
                return Message + "\n" + string.Join("\n", fields);
            }
        }
    }



}
