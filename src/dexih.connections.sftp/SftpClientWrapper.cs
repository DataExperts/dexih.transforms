// using System;
// using System.Collections.Generic;
// using System.Net.Sockets;
// using System.Reflection;
// using System.Threading;
// using System.Threading.Tasks;
// using Renci.SshNet;
// using Renci.SshNet.Sftp;
//
// namespace dexih.connections.sftp
// {
//     /// <summary>
//     /// This wrapper is due to bugs in the SSH.NET library that causes locking on OSX and Unix.
//     /// The locking is fixed by this wrapper and a custom dispose() script.
//     /// 
//     /// </summary>
//     public class SftpClientWrapper : IDisposable
//     {
//         private readonly SftpClient _client;
//         
//         public SftpClientWrapper(string serverName, string userName, string password)
//         {
//             _client = new SftpClient(serverName, userName, password);
//         }
//
//         public void Connect()
//         {
//             _client.Connect();
//         }
//
//         public void ChangeDirectory(string path)
//         {
//             _client.ChangeDirectory(path);
//         }
//
//         public bool Exists(string path)
//         {
//             return _client.Exists(path);
//         }
//
//         public void CreateDirectory(string path)
//         {
//             _client.CreateDirectory(path);
//         }
//
//         public IEnumerable<SftpFile> ListDirectory(string path, Action<int> listCallback = null)
//         {
//             return _client.ListDirectory(path, listCallback);
//         }
//
//         public SftpFileStream OpenRead (string path)
//         {
//             return _client.OpenRead(path);
//         }
//
//         public SftpFileStream OpenWrite(string path)
//         {
//             return _client.OpenWrite(path);
//         }
//
//         public void DeleteFile(string path)
//         {
//             _client.DeleteFile(path);
//         }
//
//         public void Dispose()
//         {
//             if (_client == null) return;
//
//             Task.Run(() =>
//             {
//                 //                _log.Debug("Disposing _client");
//
//                 var timer = new System.Timers.Timer
//                 {
//                     Interval = 2000,
//                     AutoReset = false
//                 };
//
//                 timer.Elapsed += (s, e) =>
//                 {
//                     try
//                     {
//                         var sessionField = _client.GetType().GetProperty("Session", BindingFlags.NonPublic | BindingFlags.Instance);
//
//                         if (sessionField != null)
//                         {
//                             var session = sessionField.GetValue(_client);
//
//                             if (session != null)
//                             {
//                                 var socketField = session.GetType().GetField("_socket", BindingFlags.NonPublic | BindingFlags.Instance);
//
//                                 if (socketField != null)
//                                 {
//                                     var socket = (Socket)socketField.GetValue(session);
//
//                                     if (socket != null)
//                                     {
// //                                        _log.Debug($"Socket state: Connected = {socket.Connected}, Blocking = {socket.Blocking}, Available = {socket.Available}, LocalEndPoint = {socket.LocalEndPoint}, RemoteEndPoint = {socket.RemoteEndPoint}");
//
// //                                        _log.Debug("Set _socket to null");
//
//                                         try
//                                         {
//                                             socket.Dispose();
//                                         }
//                                         catch (Exception)
//                                         {
// //                                            _log.Debug("Exception disposing _socket", ex);
//                                         }
//
//                                         socketField.SetValue(session, null);
//                                     }
//                                     else
//                                     {
// //                                        _log.Debug("_socket was null");
//                                     }
//                                 }
//
//                                 var messageListenerCompletedField = session.GetType().GetField("_messageListenerCompleted", BindingFlags.NonPublic | BindingFlags.Instance);
//
//                                 var messageListenerCompleted = (EventWaitHandle)messageListenerCompletedField.GetValue(session);
//
//                                 if (messageListenerCompleted != null)
//                                 {
//                                     var waitHandleSet = messageListenerCompleted.WaitOne(0);
//
// //                                    _log.Debug($"_messageListenerCompleted was set = {waitHandleSet}");
//
//                                     if (!waitHandleSet)
//                                     {
// //                                        _log.Debug($"Calling Set()");
//                                         messageListenerCompleted.Set();
//                                     }
//                                 }
//                                 else
//                                 {
// //                                    _log.Debug("_messageListenerCompleted was null");
//                                 }
//                             }
//                             else
//                             {
// //                                _log.Debug("Session was null");
//                             }
//                         }
//                     }
//                     catch (Exception)
//                     {
// //                        _log.Debug($"Exception in Timer event handler", ex);
//                     }
//                 };
//
//                 timer.Start();
//
//                 _client.Dispose();
//
// //                _log.Info("Disposed _client");
//             });
//         }
//     }
// }
