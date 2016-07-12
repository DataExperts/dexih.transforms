dotnet restore ./src/dexih.functions 
dotnet build ./src/dexih.functions -f netstandard1.6
dotnet restore ./src/dexih.transforms 
dotnet build ./src/dexih.transforms -f netstandard1.6
dotnet restore ./src/dexih.connections 
dotnet build ./src/dexih.connections -f netstandard1.6

