package pkg;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TheMapper extends Mapper<LongWritable, Text, Text, Text>
{	
	static HashMap<String,Integer> champions = new HashMap<String,Integer>();
	@Override
	protected void setup( Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	    if ( champions.size() == 0) 
	    {
			String champs = "0.1.Annie;1.2.Olaf;2.3.Galio;3.4.TwistedFate;4.5.XinZhao;5.6.Urgot;6.7.Leblanc;7.8.Vladimir;8.9.Fiddlesticks;9.10.Kayle;10.11.MasterYi;11.12.Alistar;12.13.Ryze;13.14.Sion;14.15.Sivir;15.16.Soraka;16.17.Teemo;17.18.Tristana;18.19.Warwick;19.20.Nunu;20.21.MissFortune;21.22.Ashe;22.23.Tryndamere;23.24.Jax;24.25.Morgana;25.26.Zilean;26.27.Singed;27.28.Evelynn;28.29.Twitch;29.30.Karthus;30.31.Chogath;31.32.Amumu;32.33.Rammus;33.34.Anivia;34.35.Shaco;35.36.DrMundo;36.37.Sona;37.38.Kassadin;38.39.Irelia;39.40.Janna;40.41.Gangplank;41.42.Corki;42.43.Karma;43.44.Taric;44.45.Veigar;45.48.Trundle;46.50.Swain;47.51.Caitlyn;48.53.Blitzcrank;49.54.Malphite;50.55.Katarina;51.56.Nocturne;52.57.Maokai;53.58.Renekton;54.59.JarvanIV;55.60.Elise;56.61.Orianna;57.62.MonkeyKing;58.63.Brand;59.64.LeeSin;60.67.Vayne;61.68.Rumble;62.69.Cassiopeia;63.72.Skarner;64.74.Heimerdinger;65.75.Nasus;66.76.Nidalee;67.77.Udyr;68.78.Poppy;69.79.Gragas;70.80.Pantheon;71.81.Ezreal;72.82.Mordekaiser;73.83.Yorick;74.84.Akali;75.85.Kennen;76.86.Garen;77.89.Leona;78.90.Malzahar;79.91.Talon;80.92.Riven;81.96.KogMaw;82.98.Shen;83.99.Lux;84.101.Xerath;85.102.Shyvana;86.103.Ahri;87.104.Graves;88.105.Fizz;89.106.Volibear;90.107.Rengar;91.110.Varus;92.111.Nautilus;93.112.Viktor;94.113.Sejuani;95.114.Fiora;96.115.Ziggs;97.117.Lulu;98.119.Draven;99.120.Hecarim;100.121.Khazix;101.122.Darius;102.126.Jayce;103.127.Lissandra;104.131.Diana;105.133.Quinn;106.134.Syndra;107.136.AurelionSol;108.143.Zyra;109.150.Gnar;110.154.Zac;111.157.Yasuo;112.161.Velkoz;113.163.Taliyah;114.164.Camille;115.201.Braum;116.202.Jhin;117.203.Kindred;118.222.Jinx;119.223.TahmKench;120.236.Lucian;121.238.Zed;122.240.Kled;123.245.Ekko;124.254.Vi;125.266.Aatrox;126.267.Nami;127.268.Azir;128.412.Thresh;129.420.Illaoi;130.421.RekSai;131.427.Ivern;132.429.Kalista;133.432.Bard;134.497.Rakan;135.498.Xayah";
			for(String champ: champs.split(";"))
			{
				champions.put(champ.split(".")[1], Integer.parseInt(champ.split(".")[0]));
			}
	    }
	    super.setup(context);
	}
	
	
	private String champsToString(String goodTeam, String badTeam)
	{	
		ArrayList<String> array = new ArrayList<String>();
		for(int i = 0; i < 136; i++) 
		{
			array.add("0");
		}
		
		for(String cId: goodTeam.split(":"))
		{
			array.set(champions.get(cId).intValue(), "1");
		}

		for(String cId: badTeam.split(":"))
		{
			array.set(champions.get(cId).intValue(), "-1");
		}
		
		String s = "";
		for(String champ: array)
		{
			s += champ + ",";
		}
		return s;
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		// Data structure: 
		// [0]MatchId, [1]Match Version, [2]Region, [3]Match Type, [4]Season, [5]Queue Type, 
		// [6]Bans, [7]Team 1 champs, [8]Team -1 champs, [9]Team 1 spells, [10]Team -1 spells, [11]Winner.
		
		String features = "F";
		String reducer = "R";
		
		// Split input into something real.
		
		String[] line = value.toString().split(",");
		
		// Taking the last value of the match ID as the key. This way it splits it into 10 about even splits. 
		context.write(new Text(line[0].substring(line[0].length() - 1)),  new Text(features));
	}
}