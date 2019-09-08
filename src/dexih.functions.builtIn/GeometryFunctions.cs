using NetTopologySuite.Geometries;
using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.functions.builtIn
{
    public class CoordinateXYZM: CoordinateXY
    {
        [TransformFunctionParameter(Description = "The Z-ordinate value")]
        public double Z { get; set; }

        [TransformFunctionParameter(Description = "The value of the measure")]
        public double M { get; set; }

        public CoordinateXYZM(Coordinate coordinate): base(coordinate)
        {
            if (coordinate != null)
            {
                Z = coordinate.Z;
                M = coordinate.M;
            }
        }
    }

    public class CoordinateXY
    {
        [TransformFunctionParameter(Description = "The X-ordinate value")]
        public double X { get; set; }

        [TransformFunctionParameter(Description = "The Y-ordinate value")]
        public double Y { get; set; }

        public CoordinateXY(Coordinate coordinate)
        {
            if (coordinate != null)
            {
                X = coordinate.X;
                Y = coordinate.Y;
            }
        }
    }

    public class GeometryFunctions
    {
        private List<Point> _points;
        private CoordinateList _coordinates;
        private List<LineString> _lineStrings;
        private List<Polygon> _polygons;
        private List<Geometry> _geometries;
        private int _index;

        public void Reset()
        {
            _index = 0;
            _points = null;
            _coordinates = null;
            _lineStrings = null;
            _polygons = null;
            _geometries = null;
        }
        
        public OgcGeometryType? GeometryType(Geometry geometry)
        {
            return geometry?.OgcGeometryType;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Geometry", Name = "Extract Coordinate",
        Description = "Returns the x/y/z/m coordinate from a geometry type.")]
        public CoordinateXYZM ExtractCoordinate(Geometry geometry)
        {
            if (geometry == null || geometry.Coordinate.ObjectIsNullOrBlank())
            {
                return null;
            }
            return new CoordinateXYZM(geometry.Coordinate);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Geometry", Name = "Create Point",
        Description = "Creates a new geometry as a single point.")]
        public Geometry CreatePoint(double x = double.NaN, double y = double.NaN, double z = double.NaN)
        {
            return new Point(x, y, z);
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Geometry", Name = "Create Multi-Point (from point)",
Description = "Creates a new geometry using multiple points.", ResultMethod = nameof(CreateMultiPointResult))]
        public void CreateMultiPoint(Point point)
        {
            if (_points == null)
            {
                _points = new List<Point>();
            }

            _points.Add(point);
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Geometry", Name = "Create Multi-Point (from x,y,z)",
        Description = "Creates a new geometry using multiple points.", ResultMethod =nameof(CreateMultiPointResult))]
        public void CreateMultiPointXYZ(double x = double.NaN, double y = double.NaN, double z = double.NaN)
        {
            if(_points == null)
            {
                _points = new List<Point>();
            }

            _points.Add(new Point(x, y, z));
        }

        public Geometry CreateMultiPointResult()
        {
            if(_points == null)
            {
                return null;
            }

            return new MultiPoint(_points.ToArray());
        }

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Geometry", Name = "Extract Multi-Point",
        Description = "Extracts points from a multi-point geometry.", ResetMethod = nameof(Reset))]
        public CoordinateXYZM ExtractMultiPoint(Geometry multiPoint)
        {
            if (multiPoint == null || _index >= multiPoint.Coordinates.Length)
            {
                return null;
            }

            return new CoordinateXYZM(multiPoint.Coordinates[_index++]);
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Geometry", Name = "Create LineString",
    Description = "Creates a new linestring using multiple xy coordinates.", ResultMethod = nameof(CreatLineStringResult))]
        public void CreateLineString(double x = double.NaN, double y = double.NaN)
        {
            if (_coordinates == null)
            {
                _coordinates = new CoordinateList();
            }

            _coordinates.Add(new Coordinate(x, y));
        }

        public Geometry CreatLineStringResult()
        {
            if (_coordinates == null)
            {
                return null;
            }

            return new LineString(_coordinates.ToArray());
        }


        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Geometry", Name = "Extract LineString",
        Description = "Extracts coordinates from a linestring.", ResetMethod = nameof(Reset))]
        public CoordinateXY ExtractLineString(Geometry lineString)
        {
            if (lineString == null || _index >= lineString.Coordinates.Length)
            {
                return null;
            }
            return new CoordinateXY(lineString.Coordinates[_index++]);
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Geometry", Name = "Create Polygon",
Description = "Creates a new Polygon with the given exterior boundary.", ResultMethod = nameof(CreatPolygonResult))]
        public void CreatePolygon(double x = double.NaN, double y = double.NaN)
        {
            if (_coordinates == null)
            {
                _coordinates = new CoordinateList();
            }

            _coordinates.Add(new Coordinate(x, y));
        }

        public Geometry CreatPolygonResult()
        {
            if (_coordinates == null)
            {
                return null;
            }
            var ring = new LinearRing(_coordinates.ToArray());
            return new Polygon(ring);
        }


        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Geometry", Name = "Extract Polygon",
        Description = "Extracts coordinates from a polygon geometry.", ResetMethod = nameof(Reset))]
        public CoordinateXY ExtractPolygon(Geometry polygon)
        {
            if (polygon == null || _index >= polygon.Coordinates.Length)
            {
                return null;
            }
            return new CoordinateXY(polygon.Coordinates[_index++]);
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Geometry", Name = "Create Multi-LineString",
Description = "Creates a new Multi-LineString.", ResultMethod = nameof(CreatMultiLineStringResult))]
        public void CreateMultiLineString(LineString lineString)
        {
            if (_lineStrings == null)
            {
                _lineStrings = new List<LineString>();
            }

            _lineStrings.Add(lineString);
        }

        public Geometry CreatMultiLineStringResult()
        {
            if (_lineStrings == null)
            {
                return null;
            }
            return new MultiLineString(_lineStrings.ToArray());
        }


        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Geometry", Name = "Extract Multi-LineString",
        Description = "Extracts line-strings from a multi-line-point geometry.", ResetMethod = nameof(Reset))]
        public Geometry ExtractMultiLinePoint(MultiLineString multiLineString)
        {
            if (multiLineString == null || _index >= multiLineString.Geometries.Length)
            {
                return null;
            }
            return multiLineString.Geometries[_index++];
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Geometry", Name = "Create Multi-Polygon",
Description = "Creates a new Multi-Polygon.", ResultMethod = nameof(CreatMultiPolygonResult))]
        public void CreateMultiPolygon(Polygon polygon)
        {
            if (_polygons == null)
            {
                _polygons = new List<Polygon>();
            }

            _polygons.Add(polygon);
        }

        public Geometry CreatMultiPolygonResult()
        {
            if (_polygons == null)
            {
                return null;
            }
            return new MultiPolygon(_polygons.ToArray());
        }


        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Geometry", Name = "Extract MultiPolygon",
        Description = "Extracts polygons from a Multi-Polygon geometry.", ResetMethod = nameof(Reset))]
        public Geometry ExtractMultiPolygon(MultiPolygon multiPolygons)
        {
            if (multiPolygons == null || _index >= multiPolygons.Geometries.Length)
            {
                return null;
            }
            return multiPolygons.Geometries[_index++];
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Geometry", Name = "Create Geometry Collection",
            Description = "Creates a new Geometry Collection.", ResultMethod = nameof(CreatGeometryCollectionResult))]
        public void CreateGeometryCollection(Geometry geometry)
        {
            if (_geometries == null)
            {
                _geometries = new List<Geometry>();
            }

            _geometries.Add(geometry);
        }

        public Geometry CreatGeometryCollectionResult()
        {
            if (_geometries == null)
            {
                return null;
            }
            return new GeometryCollection(_geometries.ToArray());
        }


        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Geometry", Name = "Extract Geometry Collection",
        Description = "Extracts geometries from a GeometryCollection.", ResetMethod = nameof(Reset))]
        public Geometry ExtractGeometryCollection(GeometryCollection geometryCollection)
        {
            if (geometryCollection == null || _index >= geometryCollection.Geometries.Length)
            {
                return null;
            }
            return geometryCollection.Geometries[_index++];
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Geometry", Name = "Geographic Distance",
     Description = "The distance in meters between two Geographical Coordinates. ")]
        public double GeographicDistance(double fromLatitude, double fromLongitude, double toLatitude, double toLongitude)
        {
            var rlat1 = Math.PI * fromLatitude / 180;
            var rlat2 = Math.PI * toLatitude / 180;
            var theta = fromLongitude - toLongitude;
            var rtheta = Math.PI * theta / 180;
            var dist =
                Math.Sin(rlat1) * Math.Sin(rlat2) + Math.Cos(rlat1) *
                Math.Cos(rlat2) * Math.Cos(rtheta);
            dist = Math.Acos(dist);
            dist = dist * 180 / Math.PI;
            dist = dist * 60 * 1853.159616F;

            return dist;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Geometry", Name = "Geographic Distance (points)",
     Description = "The distance in meters between two Geographical Coordinates (point x = longitude/y = latitude).")]
        public double GeographicDistancePoints(Geometry point1, Geometry point2)
        {
            if(point1 == null || point2 == null)
            {
                return double.NaN;
            }
            return GeographicDistance(point1.Coordinate.Y, point2.Coordinate.X, point2.Coordinate.Y, point2.Coordinate.X);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Geometry", Name = "Distance",
       Description = "Calculates the distance between two geometric points.")]
        public double Distance(Geometry point1, Geometry point2)
        {
            if (point1 == null || point2 == null)
            {
                return double.NaN;
            }
            return point1.Distance(point2);
        }
        
         [TransformFunction(FunctionType = EFunctionType.Map, Category = "Geometry", Name = "Is Within Distance",
        Description = "Tests whether the distance from this Geometry to another is less than or equal to a specified value.")]
        public bool IsWithinDistance(Geometry point1, Geometry point2, double distance)
        {
            if (point1 == null || point2 == null)
            {
                throw new Exception("The distance could not be calculated as on the the points is null");
            }
            return point1.IsWithinDistance(point2, distance);
        }
    }
}
