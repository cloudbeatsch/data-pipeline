using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LocationPipeline.Entities
{
    class Tile
    {
        public static long IdFromLatLong(double latitude, double longitude, int zoom)
        {
            double latitudePerTile = 180.0 / Convert.ToDouble(2 << zoom);
            double longitudePerTile = 360.0 / Convert.ToDouble(2 << zoom);

            long row = Convert.ToInt32(Math.Floor((90.0 - latitude) / latitudePerTile));
            long column = Convert.ToInt32(Math.Floor((longitude + 180.0) / longitudePerTile));

            return Tile.IdFromRowColumn(row, column, zoom);
        }

        public static long IdFromRowColumn(long row, long column, int zoom)
        {
            return BaseIdForZoom(zoom) + IndexInZoomLevel(row, column, zoom);
        }

        public static long BaseIdForZoom(int zoom) {
            long id = 0;

            for (int zoomIdx = 0; zoomIdx < zoom; zoomIdx++) {
                id += 2 << (zoomIdx * 2);
            }

            return id;
        }

        public static long IndexInZoomLevel(long row, long column, int zoom)
        {
            return (2 << zoom) * row + column;
        }
    }
}