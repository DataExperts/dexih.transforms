dotnet restore ./src/dexih.functions 
dotnet build ./src/dexih.functions -f netstandard1.6
dotnet restore ./src/dexih.transforms 
dotnet build ./src/dexih.transforms -f netstandard1.6
dotnet restore ./src/dexih.connections.azure 
dotnet build ./src/dexih.connections.azure -f netstandard1.6
dotnet restore ./src/dexih.connections.flatfile
dotnet build ./src/dexih.connections.flatfile -f netstandard1.6
dotnet restore ./src/dexih.connections.sql
dotnet build ./src/dexih.connections.sql -f netstandard1.6
dotnet restore ./src/dexih.connections.webservice.restful
dotnet build ./src/dexih.connections.webservice.restful  -f netstandard1.6
