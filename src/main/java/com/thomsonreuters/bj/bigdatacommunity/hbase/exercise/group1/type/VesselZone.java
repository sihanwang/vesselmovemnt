package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.type;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.HashMap;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.WKTReader2;

public class VesselZone implements Serializable {

	public static Geometry[] GlobalZones;

	static {
		GlobalZones = new Geometry[8];
		GeometryFactory geometryFactory = JTSFactoryFinder
				.getGeometryFactory(null);

		try {
			GlobalZones[0] = createPolygonByWKT(geometryFactory,
					"POLYGON ((0.0 0.0, 0.0 90.0, -90 90, -90.0 0.0, 0.0 0.0))");
			GlobalZones[1] = createPolygonByWKT(geometryFactory,
					"POLYGON ((-90.0 0.0, -90.0 90.0, -180.0 90.0, -180.0 0.0, -90.0 0.0))");
			GlobalZones[2] = createPolygonByWKT(geometryFactory,
					"POLYGON ((0.0 0.0, 90.0 0.0, 90.0 90.0, 0 90, 0.0 0.0))");
			GlobalZones[3] = createPolygonByWKT(geometryFactory,
					"POLYGON ((90.0 0.0, 180.0 0.0, 180.0 90.0, 90.0 90.0, 90.0 0.0))");
			GlobalZones[4] = createPolygonByWKT(geometryFactory,
					"POLYGON ((0.0 0.0, -90.0 0.0, -90.0 -90.0, 0 -90, 0.0 0.0))");
			GlobalZones[5] = createPolygonByWKT(geometryFactory,
					"POLYGON ((-90.0 0.0, -180.0 0.0, -180.0 -90.0, -90.0 -90.0, -90.0 0.0))");
			GlobalZones[6] = createPolygonByWKT(geometryFactory,
					"POLYGON ((0.0 0.0, 0.0 -90.0, 90 -90, 90.0 0.0, 0.0 0.0))");
			GlobalZones[7] = createPolygonByWKT(geometryFactory,
					"POLYGON ((90.0 0.0, 90.0 -90.0, 180.0 -90.0, 180.0 0.0, 90.0 0.0))");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static Geometry createPolygonByWKT(GeometryFactory GF, String WKT)
			throws ParseException {
		WKTReader2 reader = new WKTReader2(GF);
		Geometry polygon = reader.read(WKT);
		return polygon;
	}

	private int Axsmarine_ID;
	private String Name;
	private Geometry Polygon;
	private String ZoneType;
	private ArrayList<Integer> IntersectedGlobalZones = new ArrayList<Integer>();

	public static HashMap<Integer, VesselZone> ZoneMap = null;

	public int getAxsmarine_ID() {
		return Axsmarine_ID;
	}

	public String getName() {
		return Name;
	}

	public Geometry getPolygon() {
		return Polygon;
	}
	
	public String getZoneType()
	{
		return this.ZoneType;
	}

	public boolean IntersectedWithGlobalZone(Integer GZoneIdx) {
		return IntersectedGlobalZones.contains(GZoneIdx);
	}

	public VesselZone(int axsmarine_id, String name, Geometry polygon,
			String zonetype, ArrayList<Integer> intersectedglobalzones) {
		this.Axsmarine_ID = axsmarine_id;
		this.Name = name;
		this.Polygon = polygon;
		this.ZoneType = zonetype;
		this.IntersectedGlobalZones = intersectedglobalzones;
	}

	// //////////////////////////////////////////////////////////////////////
	// overwrite equals() method
	// //////////////////////////////////////////////////////////////////////
	public boolean equals(Object x) {
		if (x instanceof VesselZone) {
			if (((VesselZone) x).getAxsmarine_ID() == this.getAxsmarine_ID()) {
				return true;
			}
		}
		return false;
	}

	// //////////////////////////////////////////////////////////////////////
	// overwrite hashCode() method
	// //////////////////////////////////////////////////////////////////////
	public int hashCode() {
		return this.getAxsmarine_ID();
	}

	public static void main(String[] args) throws Exception {
		DiskInstance<HashMap<Integer, VesselZone>> DI = new DiskInstance<HashMap<Integer, VesselZone>>(
				"VesselZone");
		ZoneMap = DownloadAllZonesHbase();
		DI.SaveInstance(ZoneMap);

	}

	public static HashMap<Integer, VesselZone> DownloadAllZonesHbase()
			throws IOException, ParseException {

		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName
				.valueOf("cdb_vessel:vessel_polygon"));
		Scan scanzone = new Scan(); // co ScanExample-1-NewScan Create empty
									// Scan instance.
		ResultScanner zone_scanner = table.getScanner(scanzone); // co
																	// ScanExample-2-GetScanner
																	// Get a
																	// scanner
																	// to
																	// iterate
																	// over the
																	// rows.

		GeometryFactory geometryFactory = JTSFactoryFinder
				.getGeometryFactory(null);
		HashMap<Integer, VesselZone> Zones = new HashMap<Integer, VesselZone>();

		for (Result res : zone_scanner) {

			int polygon_id = Integer.parseInt(Bytes.toString(res.getRow()));
			String name = null;
			Geometry polygon = null;
			String type = null;
			ArrayList<Integer> IntersectedGlobalZones = new ArrayList<Integer>();

			for (Cell cell : res.rawCells()) {
				String Qualifier = Bytes
						.toString(CellUtil.cloneQualifier(cell));
				String Value = Bytes.toString(CellUtil.cloneValue(cell));

				if (Qualifier.equals("name")) {
					name = Value;
				} else if (Qualifier.equals("polygon")) {
					polygon = createPolygonByWKT(geometryFactory, Value);
					for (int i = 0; i < GlobalZones.length; i++) {
						if (polygon.intersects(GlobalZones[i])) {
							IntersectedGlobalZones.add(i);
						}
					}

				} else if (Qualifier.equals("type")) {
					type = Value;
				}
			}

			VesselZone thisZone = new VesselZone(polygon_id, name, polygon,	type, IntersectedGlobalZones);

			Zones.put(polygon_id, thisZone);

		}

		zone_scanner.close();
		connection.close();

		return Zones;

	}

}
