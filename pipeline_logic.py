import pandas as pd, numpy as np, requests, pytz, re, os, json, time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from email.utils import parsedate_tz, mktime_tz
from urllib.parse import urlparse
from newspaper import Article
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from fuzzywuzzy import fuzz, process
import openpyxl
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv(override=False)   # pulls in .env for dev/testing, but real env wins

def run_rss_pipeline():
    rss_feeds = [
        "http://rss.cnn.com/rss/edition_world.rss",
        "https://www.cnbc.com/id/100727362/device/rss/rss.html",
        "https://abcnews.go.com/abcnews/internationalheadlines",
        "https://www.cbsnews.com/latest/rss/world",
        "https://www.rt.com/rss/news/",
        "https://www.euronews.com/video/2025/02/21/latest-news-bulletin-february-21st-morning",
        "https://moxie.foxnews.com/google-publisher/latest.xml",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/world/rss.xml",
        "https://feeds.washingtonpost.com/rss/world",
        "https://www.cbc.ca/webfeed/rss/rss-world",
        "https://globalnews.ca/canada/feed/",
        "https://www.cbc.ca/cmlink/rss-topstories",
        "https://feeds.skynews.com/feeds/rss/world.xml",
        "http://feeds.bbci.co.uk/news/rss.xml",
        "https://www.theguardian.com/uk/rss",
        "https://feeds.feedburner.com/daily-express-world-news",
        "https://feeds.skynews.com/feeds/rss/home.xml",
        "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms",
        "https://feeds.feedburner.com/ndtvnews-world-news",
        "https://easternherald.com/feed/",
        "https://www.indiatoday.in/rss/1206578",
        "https://indianexpress.com/feed/",
        "https://www.thehindu.com/news/national/?service=rss",
        "https://www.news18.com/commonfeeds/v1/eng/rss/india.xml",
        "https://www.business-standard.com/rss/latest.rss",
        "https://www.deccanchronicle.com/google_feeds.xml",
        "https://hubnetwork.in/feed/",
        "https://kashmirnews.in/feed/",
        "https://biovoicenews.com/feed/",
        "https://www.telanganatribune.com/feed/",
        "https://starofmysore.com/feed/",
        "https://www.india.com/feed/",
        "https://www.oneindia.com/rss/news-india-fb.xml",
        "https://news.abplive.com/home/feed",
        "https://frontline.thehindu.com/cover-story/feeder/default.rss",
        "https://feeds.feedburner.com/ndtvnews-india-news",
        "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
        "https://www.news18.com/rss/india.xml",
        "https://feeds.capi24.com/v1/Search/articles/news24/World/rss",
        "https://feeds.thelocal.com/rss/es",
        "https://www.watchdoguganda.com/feed",
        "https://reporter.am/feed",
        "https://www.spiegel.de/international/index.rss",
        "https://www.faz.net/rss/aktuell/",
        "https://www.deutschland.de/en/feed-news/rss.xml",
        "https://www.kn-online.de/arc/outboundfeeds/rss/",
        "https://www.newsamericasnow.com/feed/",
        # "https://thecaribbeannewsnow.com/feed/",
        "https://www.caribbeanlife.com/feed/",
        "https://caribbeannewsglobal.com/feed/",
        # "https://jamaica-star.com/feed/news.xml",
        "https://newsday.co.tt/feed/",
        "https://repeatingislands.com/feed/",
        "https://www.reforma.com/rss/portada.xml",
        "https://vanguardia.com.mx/rss.xml",
        "https://www.elsiglodetorreon.com.mx/index.xml",
        "https://www.elfinanciero.com.mx/arc/outboundfeeds/rss/?outputType=xml",
        "https://www.sdpnoticias.com/arc/outboundfeeds/rss/?outputType=xml",
        "https://www.sinembargo.mx/feed/",
        "https://www.publimetro.com.mx/arc/outboundfeeds/rss/category/noticias/mundo/?outputType=xml",
        "https://www.elnorte.com/rss/portada.xml",
        "https://emisorasunidas.com/feed/",
        "https://crnnoticias.com/feed/",
        "https://canal2tv.com/feed/",
        # "https://www.articulo66.com/feed/",
        # "https://confidencial.digital/feed/",
        "https://lajornadanet.com/feed/",
        "https://laverdadnica.com/feed/",
        "https://thecostaricanews.com/feed/",
        "https://ticotimes.net/feed",
        "https://qcostarica.com/feed/",
        "https://www.caracaschronicles.com/feed/",
        "https://qcostarica.com/category/latest-news-from-south-america/venezuela/feed/",
        "https://www.theguardian.com/world/venezuela/rss",
        "https://latindispatch.com/category/regions/andes/venezuela/feed/",
        "https://thecitypaperbogota.com/feed/",
        "https://www.financecolombia.com/feed/",
        "https://www.minuto30.com/feed/",
        "https://www.eltiempo.com/rss/colombia.xml",
        # "https://www.lahora.com.ec/feed/",
        "https://www.elcomercio.com/feed/",
        "https://www.eldiario.ec/feed/",
        "https://www.larepublica.ec/feed/",
        "https://www.diariolosandes.com.ec/feed/",
        "https://elnorte.ec/feed/",
        "https://www.brasilwire.com/feed/",
        "https://braziljournal.com/feed/",
        "https://elcomercio.pe/arc/outboundfeeds/rss/?outputType=xml",
        "https://larazon.pe/feed/",
        "https://ojo.pe/arc/outboundfeeds/rss/?outputType=xml",
        "https://caretas.pe/feed/",
        "https://diariocorreo.pe/arc/outboundfeeds/rss/?outputType=xml",
        "https://peru21.pe/rss/",
        "https://www.df.cl/noticias/site/list/port/rss.xml",
        # "https://www.theclinic.cl/feed/",
        "https://cambio21.cl/rss",
        "https://ladiscusion.cl/feed/",
        "https://www.eldinamo.cl/feed/",
        # "https://santiagotimes.cl/feed/",
        "https://latinamericareports.com/feed/",
        "https://feeds.npr.org/1004/rss.xml",
        "https://www.batimes.com.ar/feed",
        "https://www.lanacion.com.ar/arc/outboundfeeds/rss/?outputType=xml",
        "https://feeds.feedburner.com/LaGaceta-General",
        "https://www.lavoz.com.ar/arc/outboundfeeds/feeds/rss/?outputType=xml",
        "https://www.diarioregistrado.com/rss.xml",
        "https://www.clarin.com/rss/lo-ultimo/",
        "https://www.irishcentral.com/feeds/section-articles.atom",
        "https://www.independent.ie/rss/section/ada62966-6b00-4ead-a0ba-2c179a0730b0",
        "https://www.irishmirror.ie/?service=rss",
        "https://www.irishnews.com/arc/outboundfeeds/rss/",
        "https://www.newsletter.co.uk/rss",
        "https://www.aftenposten.no/rss",
        "https://feeds.feedburner.com/newsinenglish/AzQS",
        "https://www.bt.no/rss",
        "https://www.aftenbladet.no/rss",
        "https://feeds.thelocal.com/rss/no",
        "https://www.vg.no/rss/feed/?format=rss",
        "https://www.bt.no/rss?characteristicsHotness=50..100",
        "https://www.dn.se/rss/",
        "https://www.barometern.se/feed",
        "https://www.nwt.se/feed/",
        "https://www.sydsvenskan.se/feeds/feed.xml",
        "https://feeds.expressen.se/nyheter/",
        "https://feeds.thelocal.com/rss/se",
        "https://finlandtoday.fi/feed/",
        "https://www.helsinkitimes.fi/?format=feed",
        "https://dailyfinland.fi/feed/latest-rss.xml",
        "https://www.hankasalmensanomat.fi/feed/rss",
        "https://www.themoscowtimes.com/rss/news",
        "https://tass.com/rss/v2.xml",
        "http://government.ru/en/all/rss/",
        "https://www.rt.com/rss/",
        "https://ria.ru/export/rss2/index.xml?page_type=google_newsstand",
        "https://eng.globalaffairs.ru/feed/",
        "https://www.kommersant.ru/RSS/news.xml",
        "https://www.themoscowtimes.com/rss/all",
        "https://euromaidanpress.com/feed/",
        "https://kyivindependent.com/news-archive/rss/",
        "https://en.interfax.com.ua/news/last.rss",
        "https://unn.ua/rss/news_uk.xml",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/ukraine/rss.xml",
        "https://www.pravda.com.ua/rss/",
        "https://rss.feedspot.com/ukraine_news_rss_feeds/",
        "https://rss.unian.net/site/news_ukr.rss",
        "https://www.apollo.lv/rss",
        "https://www.diena.lv/rss/",
        "https://www.delfi.lv/bizness/rss/index.xml",
        "https://news.lv/rss.xml",
        "https://www.lrytas.lt/rss",
        "https://www.15min.lt/rss",
        "https://eng.belta.by/rss",
        "https://udf.name/rss.xml",
        "https://afn.by/news/rss",
        "https://rss.gazeta.pl/pub/rss/najnowsze_wyborcza.xml",
        "https://www.rp.pl/rss_main?unknown-old-rss",
        "https://feeds.thelocal.com/rss/dk",
        "https://nltimes.nl/rssfeed2",
        "https://www.dutchnews.nl/feed/",
        "https://www.ad.nl/home/rss.xml",
        "https://www.trouw.nl/voorpagina/rss.xml",
        "https://fd.nl/?rss",
        "https://www.pzc.nl/home/rss.xml",
        "https://www.thebulletin.be/rss.xml",
        "https://brusselsmorning.com/feed/",
        "https://www.france24.com/en/rss",
        "https://www.lemonde.fr/en/rss/une.xml",
        "https://www.diplomatie.gouv.fr/spip.php?page=backend-fd",
        "https://rss.liberation.fr/rss/latest/",
        "https://www.lexpress.fr/rss/alaune.xml",
        "https://ep01.epimg.net/rss/elpais/inenglish.xml",
        "https://www.eldiario.es/rss",
        "https://feeds.thelocal.com/rss/es",
        "https://euroweeklynews.com/feed/",
        "https://eldiariony.com/feed/",
        "https://e00-elmundo.uecdn.es/elmundo/rss/portada.xml",
        "https://rss.elconfidencial.com/espana/",
        "https://www.abc.es/rss/feeds/abcPortada.xml",
        # "https://www.portugalresident.com/feed/",
        "https://feeds.feedburner.com/expresso-geral",
        "https://www.cmjornal.pt/rss",
        "https://www.ansa.it/sito/ansait_rss.xml",
        "https://www.florencedailynews.com/feed/",
        "https://www.wantedinrome.com/news?format=rss",
        "https://www.repubblica.it/rss/homepage/rss2.0.xml",
        "https://www.ilsole24ore.com/rss/italia.xml",
        "https://www.milanotoday.it/rss",
        "https://www.repubblica.it/rss/homepage/rss2.0.xml",
        "https://www.corriere.it/rss/homepage.xml",
        "https://www.ilsole24ore.com/rss/homepage.xml",
        "https://www.ansa.it/sito/notizie/topnews/topnews_rss.xml",
        "https://www.praguepost.com/feed",
        "https://praguemorning.cz/feed/",
        "https://www.ceskenoviny.cz/sluzby/rss/zpravy.php",
        "https://www.pbj.cz/feed",
        "https://servis.idnes.cz/rss.aspx",
        "https://www.sn.at/xml/rss",
        "https://www.nachrichten.at/storage/rss/rss/nachrichten.xml",
        "https://www.noen.at/xml/rss",
        "https://www.ots.at/rss/index",
        "https://www.diepresse.com/rss//",
        "https://www.24ur.com/rss",
        "https://the-slovenia.com/feed/",
        "https://www.si21.com/rss/sl/",
        "https://www1.pluska.sk/rss.xml",
        "https://spravy.pravda.sk/domace/rss/xml/",
        "https://korzar.sme.sk/rss",
        "https://www.startitup.sk/feed/",
        "https://hungarytoday.hu/feed/",
        "https://www.budapesttimes.hu/feed/",
        # "https://www.zaol.hu/feed/",
        "https://demokrata.hu/feed/",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/hungary/rss.xml",
        "https://www.dnevnik.bg/rss/",
        "https://www.novinite.com/services/news_rdf.php",
        "https://www.bta.bg/bg/rss/free",
        "https://varnautre.bg/feed/rss.xml",
        "https://feeds.feedburner.com/ekathimerini/sKip",
        "https://eleftherostypos.gr/feed",
        # "https://www.avgi.gr/rss.xml",
        "https://www.star.com.tr/rss/rss.asp?cid=124",
        "https://www.cumhuriyet.com.tr/rss/1.xml",
        "https://www.gazetevatan.com/rss/gundem.xml",
        "https://www.haber3.com/rss",
        "https://www.yenisafak.com/rss",
        # "https://csj.cumhuriyet.edu.tr/en/pub/rss/lastissue/en",
        "https://www.milliyet.com.tr/rss/rssnew/dunyarss.xml",
        "https://www.hurriyetdailynews.com/rss/news",
        "https://sana.sy/en/?feed=rss2",
        # "https://snhr.org/feed/",
        "https://www.nytimes.com/svc/collections/v1/publish/http://www.nytimes.com/topic/destination/syria/rss.xml",
        "https://www.dailymail.co.uk/news/syria/index.rss",
        "https://www.thesun.co.uk/where/syria/feed/",
        "https://www.iraq-businessnews.com/feed/",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/iraq/rss.xml",
        "https://www.executive-magazine.com/feed",
        "https://ginosblog.com/feed",
        "https://www.jordantimes.com/rss.xml",
        "https://www.haaretz.com/srv/haaretz-latest-headlines",
        "https://en.globes.co.il/WebService/Rss/RssFeeder.asmx/FeederNode?iID=942",
        "https://www.israelnationalnews.com/Rss.aspx?act=.1",
        "https://www.israelnationalnews.com/Rss.aspx",
        "https://www.aljazeera.com/xml/rss/all.xml",
        "https://www.jpost.com/Rss/RssFeedsHeadlines.aspx",
        # "https://www.timesofisrael.com/feed/",
        "https://timeskuwait.com/news/feed/",
        "https://kuwaitnews.com/feed/",
        "https://www.arabnews.com/rss.xml",
        "https://saudigazette.com.sa/rssFeed/74",
        "https://www.emirates247.com/cmlink/rss-feed-1.4268?localLinksEnabled=false",
        "https://www.dubaichronicle.com/feed/",
        "https://uae24x7.com/feed/",
        "https://www.emirates247.com/cmlink/izooto-1.712150?ot=ot.AjaxPageLayout",
        "https://www.dailynewsegypt.com/feed/",
        "https://www.madamasr.com/en/feed/",
        # "https://www.arabianbusiness.com/gcc/oman/feed",
        "https://www.arabnews.com/taxonomy/term/2546/feed",
        "https://themedialine.org/tag/yemen/feed/",
        "https://www.egyptindependent.com/feed/",
        "https://www.masress.com/en/rss",
        # "https://www.madamasr.com/en/feed/",
        "https://egyptian-gazette.com/feed/",
        "https://en.mehrnews.com/rss",
        "https://www.tehrantimes.com/rss",
        # "https://iranprimer.usip.org/rss.xml",
        "https://feeds.iranherald.com/rss/1b76a2b4cf7810bd",
        "https://www.dawn.com/feeds/home",
        "https://www.urdupoint.com/en/sitemap/news.rss",
        "https://feeds.afghanistannews.net/rss/6e1d5c8e1f98f17c",
        # "https://pajhwok.com/feed/",
        "https://www.aopnews.com/feed/",
        "https://www.hronikatm.com/feed/",
        "https://feeds.turkmenistannews.net/rss/929bcf2071e81801",
        "https://feeds.feedburner.com/NewsCentralAsia",
        "https://orient.tm/feed/",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/uzbekistan/rss.xml",
        "https://astanatimes.com/feed/atom/",
        "https://www.inform.kz/rss/eng.xml",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/kazakhstan/rss.xml",
        "https://chinadigitaltimes.net/feed/",
        "https://technode.com/feed/",
        "https://feeds.bbci.co.uk/news/world/asia/china/rss.xml",
        "https://thediplomat.com/category/china-power/feed/",
        "https://china-environment-news.net/feed/",
        "https://www.nytimes.com/svc/collections/v1/publish/http://www.nytimes.com/topic/destination/china/rss.xml",
        "https://feeds.beijingbulletin.com/rss/55582c89cb296d4c",
        "https://www.scmp.com/rss/91/feed",
        "https://feeds.northkoreatimes.com/rss/08aysdf7tga9s7f7",
        "https://www.koreatimes.co.kr/www/rss/rss.xml",
        "https://en.yna.co.kr/RSS/news.xml",
        "https://english.hani.co.kr/rss/english_edition",
        "https://www.japantimes.co.jp/feed/",
        "https://japantoday.com/feed/atom",
        "https://newsonjapan.com/rss/top.xml",
        "https://english.kyodonews.net/rss/all.xml",
        "https://www.nytimes.com/svc/collections/v1/publish/http://www.nytimes.com/topic/destination/japan/rss.xml",
        "https://feeds.thejapannews.net/rss/c4f2dd8ca8c78044",
        "https://feeds.feedburner.com/rsscna/engnews",
        "https://api.taiwantoday.tw/en/rss.php?unit=2,6,10,15,18",
        "https://en.antaranews.com/rss/news.xml",
        "https://feeds.indonesianews.net/rss/f9295dc05093c851",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/indonesia/rss.xml",
        "https://www.ft.com/indonesia?format=rss",
        "https://www.sindonews.com/feed",
        "https://wartakota.tribunnews.com/rss",
        "https://www.jpnn.com/index.php?mib=rss",
        "https://rss.tempo.co/",
        "https://www.cnnindonesia.com/rss",
        "https://theindependent.sg/feed/",
        "https://feeds.feedburner.com/hardwarezone/all",
        "https://feeds.singaporestar.com/rss/a677a0718b69db72",
        "http://feeds.singaporenews.net/rss/a677a0718b69db72",
        "https://www.malaymail.com/feed/rss/malaysia",
        "https://www.utusan.com.my/feed/",
        # "https://www.themalaysianinsight.com/feed",
        "https://www.bernama.com/en/rssfeed.php",
        "https://www.sarawakreport.org/feed/",
        "https://news.pngfacts.com/feeds/posts/default?alt=rss",
        "https://www.abc.net.au/news/feed/2942460/rss.xml",
        "https://www.9news.com.au/rss",
        "https://www.sbs.com.au/feed/news/content-collection-rss/top-stories",
        "https://feeds.feedburner.com/IndependentAustralia",
        "https://www.smh.com.au/rss/world.xml",
        "https://www.sbs.com.au/news/feed",
        "https://www.abc.net.au/news/feed/51120/rss.xml",
        "https://www.stuff.co.nz/rss",
        "https://www.rnz.co.nz/rss/national.xml",
        "https://thedailyblog.co.nz/category/daily_blogs/feed/",
        "https://fijisun.com.fj/feed/",
        "https://feeds.thefijinews.net/rss/7b9fd5fd3be1c082",
        "https://nzfijitimes.co.nz/feed/",
        "https://www.madagascar-tribune.com/spip.php?page=backend",
        # "https://newsmada.com/feed/",
        "https://e.vnexpress.net/rss/news.rss",
        "https://www.khaosodenglish.com/feed/",
        # "https://prachataienglish.com/rss.xml",
        # "https://www.frontiermyanmar.net/en/feed/",
        "https://kachinnews.com/feed/?doing_wp_cron=1740036499.2181890010833740234375",
        "https://www.jagonews24.com/rss/rss.xml",
        # "https://www.onlinekhabar.com/feed",
        "https://rajdhanidaily.com/feed/",
        "https://www.oujdacity.net/feed",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/morocco/rss.xml",
        "https://lecalame.info/?q=rss.xml",
        # "https://bamada.net/feed",
        "https://www.altaghyeer.info/en/feed/",
        "https://www.dabangasudan.org/en/feed",
        "http://feeds.sudannews.net/rss/c1ab2109a5bf37ec",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/somalia/rss.xml",
        "https://sonna.so/en/feed/",
        "https://www.theguardian.com/world/somalia/rss",
        "https://thehabesha.com/feed/",
        "http://www.tadias.com/feed/atom/",
        "https://www.kenyans.co.ke/feeds/news?_wrapper_format=html",
        # "https://www.businessdailyafrica.com/service/rss/bd/1939132/feed.rss",
        # "https://www.theeastafrican.co.ke/service/rss/tea/1289142/feed.rss",
        "https://www.kenyanwhispers.co.ke/feed/",
        "https://www.4x4uganda.com/feed",
        "https://anchor.fm/s/5a74f2e8/podcast/rss",
        "https://exclusive.co.ug/feed/",
        # "https://www.dignited.com/feed/",
        "https://scribe.co.ug/feed/",
        "https://capsud.net/feed/",
        # "https://taarifa.rw/feed/",
        # "https://burundi-eco.com/feed/",
        # "https://www.mwananchi.co.tz/service/rss/mw/2733734/feed.rss",
        "https://mtanzania.co.tz/feed/",
        "https://www.tanzaniainvest.com/feed",
        "https://www.thecitizen.co.tz/service/rss/tanzania/2486554/feed.rss",
        # "https://www.club-k.net/index.php?option=com_obrss&task=feed&id=2:rss-noticias-do-club-k&format=feed&lang=pt",
        "https://www.opais.ao/feed/",
        "https://jornalf8.net/feed/",
        "https://www.times.co.zm/?feed=rss2",
        "https://lusakastar.com/feed",
        "https://lusakavoice.com/feed/",
        "https://www.mwebantu.com/feed/",
        "https://diggers.news/feed/",
        "https://zambianews365.com/feed/",
        "https://www.faceofmalawi.com/feed/",
        "https://www.maraviexpress.com/feed/",
        "https://times.mw/feed/",
        "https://malawi24.com/feed/",
        "https://www.maravipost.com/feed/",
        "https://malawifreedomnetwork.com/feed/",
        "https://www.jornaldomingo.co.mz/feed/",
        "https://www.chronicle.co.zw/feed/",
        # "https://www.sundaymail.co.zw/feed",
        "https://iharare.com/feed/",
        "https://mbaretimes.com/feed/",
        "https://www.myzimbabwe.co.zw/feed",
        "https://botswanaunplugged.com/feed/",
        "https://news.thevoicebw.com/feed/",
        "https://guardiansun.co.bw/rssFeed/48",
        "https://www.namibian.com.na/feed/",
        # "https://economist.com.na/feed/",
        "https://www.observer24.com.na/feed/",
        "https://namibiadailynews.info/feed/",
        "https://feeds.capi24.com/v1/Search/articles/News24/TopStories/rss",
        "https://www.dailymaverick.co.za/dmrss/",
        "https://www.sowetanlive.co.za/rss/?publication=sowetan-live",
        "https://rss.iol.io/iol/news",
        "https://www.citizen.co.za/feed/",
        # "https://businesstech.co.za/news/feed/",
        "https://www.thesouthafrican.com/feed/",
        "https://www.africanews.com/feed/rss?themes=news",
        "https://www.timeslive.co.za/rss/",
        "https://www.businesslive.co.za/rss/",
        "https://www.vanguardngr.com/feed/",
        "https://guardian.ng/feed/",
        "https://www.premiumtimesng.com/feed",
        "https://rss.punchng.com/v1/category/latest_news",
        # "https://pmnewsnigeria.com/feed/",
        "https://dailypost.ng/feed/",
        # "https://www.channelstv.com/feed/",
        "https://punchng.com/feed/",
        "https://www.pulse.com.gh/rss",
        "https://ghheadlines.com/rss",
        "https://theheraldghana.com/feed/",
        # "https://guineematin.com/feed/",
        # "https://guineelive.com/feed/",
        "https://www.lejourguinee.com/feed/",
        "https://aminata.com/feed/",
        "https://feeds.content.dowjones.io/public/rss/RSSWorldNews",
        "https://feeds.content.dowjones.io/public/rss/RSSUSnews",
        "https://feeds.content.dowjones.io/public/rss/socialpoliticsfeed",
        "http://www.xinhuanet.com/english/rss/worldrss.xml",
        "http://www.xinhuanet.com/english/rss/chinarss.xml",
        "https://www3.nhk.or.jp/rss/news/cat0.xml",
        "https://feeds.feedburner.com/NDTV-LatestNews",
        "https://www.republicworld.com/rss/india.xml",
        "https://en.yna.co.kr/RSS/news.xml",
        "https://en.yna.co.kr/RSS/national.xml",
        "https://en.yna.co.kr/RSS/nk.xml",
        "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/canada/",
        "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/world/",
        "https://rss.jpost.com/rss/rssnewera",
        "https://rss.jpost.com/rss/rssnews-updates",
        "https://rss.jpost.com/rss/rssfeedsisraelnews.aspx",
        "https://rss.jpost.com/rss/rssfeedsinternational",
        "https://www.timesofisrael.com/feed/",
        "https://www.tasnimnews.com/en/rss/feed/0/7/1194/world",
        "https://www.tasnimnews.com/en/rss/feed/0/7/0/all-stories",
        "https://feeds.feedburner.com/geo/GiKR",
        "https://feeds.feedburner.com/geo/wUIl",
        "https://www.dailynewsegypt.com/feed/",
        "https://mg.co.za/feed/",
        "https://www.lavanguardia.com/rss/internacional.xml",
        "https://www.lavanguardia.com/rss/home.xml",
        "https://tuoitre.vn/rss/tin-moi-nhat.rss",
        "https://danviet.vn/rss/home.rss",
        "https://www.bangkokpost.com/rss/data/world.xml",
        "https://www.bangkokpost.com/rss/data/topstories.xml",
        "https://www.bangkokpost.com/rss/data/thailand.xml",
        "https://www.newlyswissed.com/feed/",
        "https://lenews.ch/feed/",
        "https://www.nrk.no/norge/toppsaker.rss",
        "https://feeds.feedburner.com/kathimerini/DJpy",
        "https://www.straitstimes.com/news/singapore/rss.xml",
        "https://www.straitstimes.com/news/world/rss.xml",
        "https://www.standaard.be/rss/section/1f2838d4-99ea-49f0-9102-138784c7ea7c",
        "https://www.lesoir.be/rss2/2/cible_principale",
        "https://feeds.feedburner.com/publicoRSS",
        "https://addisstandard.com/feed/",
        "https://www.elnacional.com/feed/",
        "https://ultimasnoticias.com.ve/feed/",
        "https://www.telegraph.co.uk/rss.xml",
        "https://rss.dw.com/rdf/rss-en-world",
        "https://rss.dw.com/rdf/rss-en-ger",
        "https://rss.dw.com/rdf/rss-en-top"
    ]

    REGION_TIMEZONES = {
        "USA": "America/New_York",
        "Canada": "America/Toronto",
        "UK": "Europe/London",
        "India": "Asia/Kolkata",
        "China": "Asia/Shanghai",
        "Japan": "Asia/Tokyo",
        "South Korea": "Asia/Seoul",
        "North Korea": "Asia/Pyongyang",
        "Russia": "Europe/Moscow",
        "Germany": "Europe/Berlin",
        "France": "Europe/Paris",
        "Spain": "Europe/Madrid",
        "Italy": "Europe/Rome",
        "Netherlands": "Europe/Amsterdam",
        "Belgium": "Europe/Brussels",
        "Switzerland": "Europe/Zurich",
        "Austria": "Europe/Vienna",
        "Poland": "Europe/Warsaw",
        "Denmark": "Europe/Copenhagen",
        "Norway": "Europe/Oslo",
        "Sweden": "Europe/Stockholm",
        "Finland": "Europe/Helsinki",
        "Ukraine": "Europe/Kiev",
        "Turkey": "Europe/Istanbul",
        "Israel": "Asia/Jerusalem",
        "Pakistan": "Asia/Karachi",
        "Afghanistan": "Asia/Kabul",
        "Indonesia": "Asia/Jakarta",
        "Malaysia": "Asia/Kuala_Lumpur",
        "Singapore": "Asia/Singapore",
        "Thailand": "Asia/Bangkok",
        "Vietnam": "Asia/Ho_Chi_Minh",
        "Australia": "Australia/Sydney",
        "New Zealand": "Pacific/Auckland",
        "South Africa": "Africa/Johannesburg",
        "Nigeria": "Africa/Lagos",
        "Egypt": "Africa/Cairo",
        "Ethiopia": "Africa/Addis_Ababa",
        "Kenya": "Africa/Nairobi",
        "Brazil": "America/Sao_Paulo",
        "Mexico": "America/Mexico_City",
        "Argentina": "America/Argentina/Buenos_Aires",
        "Chile": "America/Santiago",
        "Peru": "America/Lima",
        "Venezuela": "America/Caracas",
        "Colombia": "America/Bogota",
        "World": "UTC",  # Default for international/global sources
    }

    # Creating a list of country-RSS link pairs
    data = [
        ("World", "http://rss.cnn.com/rss/edition_world.rss"),
        ("World", "https://www.cnbc.com/id/100727362/device/rss/rss.html"),
        ("World", "https://abcnews.go.com/abcnews/internationalheadlines"),
        ("World", "https://www.cbsnews.com/latest/rss/world"),
        ("World", "https://www.rt.com/rss/news/"),
        ("World", "https://www.euronews.com/video/2025/02/21/latest-news-bulletin-february-21st-morning"),
        ("USA", "https://moxie.foxnews.com/google-publisher/latest.xml"),
        ("USA", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/world/rss.xml"),
        ("USA", "https://feeds.washingtonpost.com/rss/world"),
        ("Canada", "https://www.cbc.ca/webfeed/rss/rss-world"),
        ("Canada", "https://globalnews.ca/canada/feed/"),
        ("Canada", "https://www.cbc.ca/cmlink/rss-topstories"),
        ("UK", "https://feeds.skynews.com/feeds/rss/world.xml"),
        ("UK", "http://feeds.bbci.co.uk/news/rss.xml"),
        ("UK", "https://www.theguardian.com/uk/rss"),
        ("UK", "https://feeds.feedburner.com/daily-express-world-news"),
        ("UK", "https://feeds.skynews.com/feeds/rss/home.xml"),
        ("India", "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms"),
        ("India", "https://feeds.feedburner.com/ndtvnews-world-news"),
        ("India", "https://easternherald.com/feed/"),
        ("India", "https://www.indiatoday.in/rss/1206578"),
        ("India", "https://indianexpress.com/feed/"),
        ("India", "https://www.thehindu.com/news/national/?service=rss"),
        ("India", "https://www.news18.com/commonfeeds/v1/eng/rss/india.xml"),
        # ("India", "https://www.business-standard.com/rss/latest.rss"),
        ("India", "https://www.deccanchronicle.com/google_feeds.xml"),
        ("India", "https://hubnetwork.in/feed/"),
        ("India", "https://kashmirnews.in/feed/"),
        ("India", "https://biovoicenews.com/feed/"),
        ("India", "https://www.telanganatribune.com/feed/"),
        ("India", "https://starofmysore.com/feed/"),
        ("India", "https://www.india.com/feed/"),
        ("India", "https://www.oneindia.com/rss/news-india-fb.xml"),
        ("India", "https://news.abplive.com/home/feed"),
        ("India", "https://frontline.thehindu.com/cover-story/feeder/default.rss"),
        ("India", "https://feeds.feedburner.com/ndtvnews-india-news"),
        ("India", "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms"),
        ("India", "https://www.news18.com/rss/india.xml"),
        ("Africa", "https://feeds.capi24.com/v1/Search/articles/news24/World/rss"),
        ("Spain", "https://feeds.thelocal.com/rss/es"),
        ("Uganda", "https://www.watchdoguganda.com/feed"),
        ("Armenia", "https://reporter.am/feed"),
        ("Germany", "https://www.spiegel.de/international/index.rss"),
        ("Germany", "https://www.faz.net/rss/aktuell/"),
        ("Germany", "https://www.deutschland.de/en/feed-news/rss.xml"),
        ("Germany", "https://www.kn-online.de/arc/outboundfeeds/rss/"),
        ("Caribbean and Latin America", "https://www.newsamericasnow.com/feed/"),
        ("Caribbean and Latin America", "https://thecaribbeannewsnow.com/feed/"),
        ("Caribbean and Latin America", "https://www.caribbeanlife.com/feed/"),
        ("Caribbean and Latin America", "https://caribbeannewsglobal.com/feed/"),
        ("Caribbean and Latin America", "https://jamaica-star.com/feed/news.xml"),
        ("Caribbean and Latin America", "https://newsday.co.tt/feed/"),
        ("Caribbean and Latin America", "https://repeatingislands.com/feed/"),
        ("Mexico", "https://www.reforma.com/rss/portada.xml"),
        ("Mexico", "https://vanguardia.com.mx/rss.xml"),
        ("Mexico", "https://www.elsiglodetorreon.com.mx/index.xml"),
        ("Mexico", "https://www.elfinanciero.com.mx/arc/outboundfeeds/rss/?outputType=xml"),
        ("Mexico", "https://www.sdpnoticias.com/arc/outboundfeeds/rss/?outputType=xml"),
        ("Mexico", "https://www.sinembargo.mx/feed/"),
        ("Mexico", "https://www.publimetro.com.mx/arc/outboundfeeds/rss/category/noticias/mundo/?outputType=xml"),
        ("Mexico", "https://www.elnorte.com/rss/portada.xml"),
        ("Guatemala", "https://emisorasunidas.com/feed/"),
        ("Guatemala", "https://crnnoticias.com/feed/"),
        ("Nicaragua", "https://canal2tv.com/feed/"),
        ("Nicaragua", "https://www.articulo66.com/feed/"),
        ("Nicaragua", "https://confidencial.digital/feed/"),
        ("Nicaragua", "https://lajornadanet.com/feed/"),
        ("Nicaragua", "https://laverdadnica.com/feed/"),
        ("Costa Rica", "https://thecostaricanews.com/feed/"),
        ("Costa Rica", "https://ticotimes.net/feed"),
        ("Costa Rica", "https://qcostarica.com/feed/"),
        ("Venezuela", "https://www.caracaschronicles.com/feed/"),
        ("Venezuela", "https://qcostarica.com/category/latest-news-from-south-america/venezuela/feed/"),
        ("Venezuela", "https://www.theguardian.com/world/venezuela/rss"),
        ("Venezuela", "https://latindispatch.com/category/regions/andes/venezuela/feed/"),
        ("Colombia", "https://thecitypaperbogota.com/feed/"),
        ("Colombia", "https://www.financecolombia.com/feed/"),
        ("Colombia", "https://www.minuto30.com/feed/"),
        ("Colombia", "https://www.eltiempo.com/rss/colombia.xml"),
        ("Ecuador", "https://www.lahora.com.ec/feed/"),
        ("Ecuador", "https://www.elcomercio.com/feed/"),
        ("Ecuador", "https://www.eldiario.ec/feed/"),
        ("Ecuador", "https://www.larepublica.ec/feed/"),
        ("Ecuador", "https://www.diariolosandes.com.ec/feed/"),
        ("Ecuador", "https://elnorte.ec/feed/"),
        ("Brazil", "https://www.brasilwire.com/feed/"),
        ("Brazil", "https://braziljournal.com/feed/"),
        ("Peru", "https://elcomercio.pe/arc/outboundfeeds/rss/?outputType=xml"),
        ("Peru", "https://larazon.pe/feed/"),
        ("Peru", "https://ojo.pe/arc/outboundfeeds/rss/?outputType=xml"),
        ("Peru", "https://caretas.pe/feed/"),
        ("Peru", "https://diariocorreo.pe/arc/outboundfeeds/rss/?outputType=xml"),
        ("Peru", "https://peru21.pe/rss/"),
        ("Chile", "https://www.df.cl/noticias/site/list/port/rss.xml"),
        ("Chile", "https://www.theclinic.cl/feed/"),
        ("Chile", "https://cambio21.cl/rss"),
        ("Chile", "https://ladiscusion.cl/feed/"),
        ("Chile", "https://www.eldinamo.cl/feed/"),
        ("Chile", "https://santiagotimes.cl/feed/"),
        ("Latin America", "https://latinamericareports.com/feed/"),
        ("Latin America", "https://feeds.npr.org/1004/rss.xml"),
        ("Argentina", "https://www.batimes.com.ar/feed"),
        ("Argentina", "https://www.lanacion.com.ar/arc/outboundfeeds/rss/?outputType=xml"),
        ("Argentina", "https://feeds.feedburner.com/LaGaceta-General"),
        ("Argentina", "https://www.lavoz.com.ar/arc/outboundfeeds/feeds/rss/?outputType=xml"),
        ("Argentina", "https://www.diarioregistrado.com/rss.xml"),
        ("Argentina", "https://www.clarin.com/rss/lo-ultimo/"),
        ("Ireland", "https://www.irishcentral.com/feeds/section-articles.atom"),
        ("Ireland", "https://www.independent.ie/rss/section/ada62966-6b00-4ead-a0ba-2c179a0730b0"),
        ("Ireland", "https://www.irishmirror.ie/?service=rss"),
        ("Ireland", "https://www.irishnews.com/arc/outboundfeeds/rss/"),
        ("Ireland", "https://www.newsletter.co.uk/rss"),
        ("Norway", "https://www.aftenposten.no/rss"),
        ("Norway", "https://feeds.feedburner.com/newsinenglish/AzQS"),
        ("Norway", "https://www.bt.no/rss"),
        ("Norway", "https://www.aftenbladet.no/rss"),
        ("Norway", "https://feeds.thelocal.com/rss/no"),
        ("Norway", "https://www.vg.no/rss/feed/?format=rss"),
        ("Norway", "https://www.bt.no/rss?characteristicsHotness=50..100"),
        ("Sweden", "https://www.dn.se/rss/"),
        ("Sweden", "https://www.barometern.se/feed"),
        ("Sweden", "https://www.nwt.se/feed/"),
        ("Sweden", "https://www.sydsvenskan.se/feeds/feed.xml"),
        ("Sweden", "https://feeds.expressen.se/nyheter/"),
        ("Sweden", "https://feeds.thelocal.com/rss/se"),
        ("Finland", "https://finlandtoday.fi/feed/"),
        ("Finland", "https://www.helsinkitimes.fi/?format=feed"),
        ("Finland", "https://dailyfinland.fi/feed/latest-rss.xml"),
        ("Finland", "https://www.hankasalmensanomat.fi/feed/rss"),
        ("Russia", "https://www.themoscowtimes.com/rss/news"),
        ("Russia", "https://tass.com/rss/v2.xml"),
        ("Russia", "http://government.ru/en/all/rss/"),
        ("Russia", "https://www.rt.com/rss/"),
        ("Russia", "https://ria.ru/export/rss2/index.xml?page_type=google_newsstand"),
        ("Russia", "https://eng.globalaffairs.ru/feed/"),
        ("Russia", "https://www.kommersant.ru/RSS/news.xml"),
        ("Russia", "https://www.themoscowtimes.com/rss/all"),
        ("Ukraine", "https://euromaidanpress.com/feed/"),
        ("Ukraine", "https://kyivindependent.com/news-archive/rss/"),
        ("Ukraine", "https://en.interfax.com.ua/news/last.rss"),
        ("Ukraine", "https://unn.ua/rss/news_uk.xml"),
        ("Ukraine", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/ukraine/rss.xml"),
        ("Ukraine", "https://www.pravda.com.ua/rss/"),
        ("Ukraine", "https://rss.feedspot.com/ukraine_news_rss_feeds/"),
        ("Ukraine", "https://rss.unian.net/site/news_ukr.rss"),
        ("Latvia", "https://www.apollo.lv/rss"),
        ("Latvia", "https://www.diena.lv/rss/"),
        ("Latvia", "https://www.delfi.lv/bizness/rss/index.xml"),
        ("Latvia", "https://news.lv/rss.xml"),
        ("Lithuania", "https://www.lrytas.lt/rss"),
        ("Lithuania", "https://www.15min.lt/rss"),
        ("Belarus", "https://eng.belta.by/rss"),
        ("Belarus", "https://udf.name/rss.xml"),
        ("Belarus", "https://afn.by/news/rss"),
        ("Poland", "https://rss.gazeta.pl/pub/rss/najnowsze_wyborcza.xml"),
        ("Poland", "https://www.rp.pl/rss_main?unknown-old-rss"),
        ("Denmark", "https://feeds.thelocal.com/rss/dk"),
        ("Netherlands", "https://nltimes.nl/rssfeed2"),
        ("Netherlands", "https://www.dutchnews.nl/feed/"),
        ("Netherlands", "https://www.ad.nl/home/rss.xml"),
        ("Netherlands", "https://www.trouw.nl/voorpagina/rss.xml"),
        ("Netherlands", "https://fd.nl/?rss"),
        ("Netherlands", "https://www.pzc.nl/home/rss.xml"),
        ("Belgium", "https://www.thebulletin.be/rss.xml"),
        ("Belgium", "https://brusselsmorning.com/feed/"),
        ("France", "https://www.france24.com/en/rss"),
        ("France", "https://www.lemonde.fr/en/rss/une.xml"),
        ("France", "https://www.diplomatie.gouv.fr/spip.php?page=backend-fd"),
        ("France", "https://rss.liberation.fr/rss/latest/"),
        ("France", "https://www.lexpress.fr/rss/alaune.xml"),
        ("Spain", "https://ep01.epimg.net/rss/elpais/inenglish.xml"),
        ("Spain", "https://www.eldiario.es/rss"),
        ("Spain", "https://feeds.thelocal.com/rss/es"),
        ("Spain", "https://euroweeklynews.com/feed/"),
        ("Spain", "https://eldiariony.com/feed/"),
        ("Spain", "https://e00-elmundo.uecdn.es/elmundo/rss/portada.xml"),
        ("Spain", "https://rss.elconfidencial.com/espana/"),
        ("Spain", "https://www.abc.es/rss/feeds/abcPortada.xml"),
        ("Portugal", "https://www.portugalresident.com/feed/"),
        ("Portugal", "https://feeds.feedburner.com/expresso-geral"),
        ("Portugal", "https://www.cmjornal.pt/rss"),
        ("Italy", "https://www.ansa.it/sito/ansait_rss.xml"),
        ("Italy", "https://www.florencedailynews.com/feed/"),
        ("Italy", "https://www.wantedinrome.com/news?format=rss"),
        ("Italy", "https://www.repubblica.it/rss/homepage/rss2.0.xml"),
        ("Italy", "https://www.ilsole24ore.com/rss/italia.xml"),
        ("Italy", "https://www.milanotoday.it/rss"),
        ("Italy", "https://www.repubblica.it/rss/homepage/rss2.0.xml"),
        ("Italy", "https://www.corriere.it/rss/homepage.xml"),
        ("Italy", "https://www.ilsole24ore.com/rss/homepage.xml"),
        ("Italy", "https://www.ansa.it/sito/notizie/topnews/topnews_rss.xml"),
        ("Czech Republic", "https://www.praguepost.com/feed"),
        ("Czech Republic", "https://praguemorning.cz/feed/"),
        ("Czech Republic", "https://www.ceskenoviny.cz/sluzby/rss/zpravy.php"),
        ("Czech Republic", "https://www.pbj.cz/feed"),
        ("Czech Republic", "https://servis.idnes.cz/rss.aspx"),
        ("Austria", "https://www.sn.at/xml/rss"),
        ("Austria", "https://www.nachrichten.at/storage/rss/rss/nachrichten.xml"),
        ("Austria", "https://www.noen.at/xml/rss"),
        ("Austria", "https://www.ots.at/rss/index"),
        ("Austria", "https://www.diepresse.com/rss//"),
        ("Slovenia", "https://www.24ur.com/rss"),
        ("Slovenia", "https://the-slovenia.com/feed/"),
        ("Slovenia", "https://www.si21.com/rss/sl/"),
        ("Slovakia", "https://www1.pluska.sk/rss.xml"),
        ("Slovakia", "https://spravy.pravda.sk/domace/rss/xml/"),
        ("Slovakia", "https://korzar.sme.sk/rss"),
        ("Slovakia", "https://www.startitup.sk/feed/"),
        ("Hungary", "https://hungarytoday.hu/feed/"),
        ("Hungary", "https://www.budapesttimes.hu/feed/"),
        ("Hungary", "https://www.zaol.hu/feed/"),
        ("Hungary", "https://demokrata.hu/feed/"),
        ("Hungary", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/hungary/rss.xml"),
        ("Bulgaria", "https://www.dnevnik.bg/rss/"),
        ("Bulgaria", "https://www.novinite.com/services/news_rdf.php"),
        ("Bulgaria", "https://www.bta.bg/bg/rss/free"),
        ("Bulgaria", "https://varnautre.bg/feed/rss.xml"),
        ("Greece", "https://feeds.feedburner.com/ekathimerini/sKip"),
        ("Greece", "https://eleftherostypos.gr/feed"),
        ("Greece", "https://www.avgi.gr/rss.xml"),
        ("Turkey", "https://www.star.com.tr/rss/rss.asp?cid=124"),
        ("Turkey", "https://www.cumhuriyet.com.tr/rss/1.xml"),
        ("Turkey", "https://www.gazetevatan.com/rss/gundem.xml"),
        ("Turkey", "https://www.haber3.com/rss"),
        ("Turkey", "https://www.yenisafak.com/rss"),
        ("Turkey", "https://csj.cumhuriyet.edu.tr/en/pub/rss/lastissue/en"),
        ("Turkey", "https://www.milliyet.com.tr/rss/rssnew/dunyarss.xml"),
        ("Turkey", "https://www.hurriyetdailynews.com/rss/news"),
        ("Syria", "https://sana.sy/en/?feed=rss2"),
        ("Syria", "https://snhr.org/feed/"),
        ("Syria", "https://www.nytimes.com/svc/collections/v1/publish/http://www.nytimes.com/topic/destination/syria/rss.xml"),
        ("Syria", "https://www.dailymail.co.uk/news/syria/index.rss"),
        ("Syria", "https://www.thesun.co.uk/where/syria/feed/"),
        ("Iraq", "https://www.iraq-businessnews.com/feed/"),
        ("Iraq", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/iraq/rss.xml"),
        ("Lebanon", "https://www.executive-magazine.com/feed"),
        ("Lebanon", "https://ginosblog.com/feed"),
        ("Jordan", "https://www.jordantimes.com/rss.xml"),
        ("Israel", "https://www.haaretz.com/srv/haaretz-latest-headlines"),
        ("Israel", "https://en.globes.co.il/WebService/Rss/RssFeeder.asmx/FeederNode?iID=942"),
        ("Israel", "https://www.israelnationalnews.com/Rss.aspx?act=.1"),
        ("Israel", "https://www.israelnationalnews.com/Rss.aspx"),
        ("Israel", "https://www.aljazeera.com/xml/rss/all.xml"),
        ("Israel", "https://www.jpost.com/Rss/RssFeedsHeadlines.aspx"),
        ("Israel", "https://www.timesofisrael.com/feed/"),
        ("Kuwait", "https://timeskuwait.com/news/feed/"),
        ("Kuwait", "https://kuwaitnews.com/feed/"),
        ("Saudi Arabia", "https://www.arabnews.com/rss.xml"),
        ("Saudi Arabia", "https://saudigazette.com.sa/rssFeed/74"),
        ("UAE", "https://www.emirates247.com/cmlink/rss-feed-1.4268?localLinksEnabled=false"),
        ("UAE", "https://www.dubaichronicle.com/feed/"),
        ("UAE", "https://uae24x7.com/feed/"),
        ("UAE", "https://www.emirates247.com/cmlink/izooto-1.712150?ot=ot.AjaxPageLayout"),
        ("UAE", "https://www.dailynewsegypt.com/feed/"),
        ("UAE", "https://www.madamasr.com/en/feed/"),
        ("Oman", "https://www.arabianbusiness.com/gcc/oman/feed"),
        ("Yemen", "https://www.arabnews.com/taxonomy/term/2546/feed"),
        ("Yemen", "https://themedialine.org/tag/yemen/feed/"),
        ("Egypt", "https://www.egyptindependent.com/feed/"),
        ("Egypt", "https://www.masress.com/en/rss"),
        ("Egypt", "https://www.madamasr.com/en/feed/"),
        ("Egypt", "https://egyptian-gazette.com/feed/"),
        ("Iran", "https://en.mehrnews.com/rss"),
        ("Iran", "https://www.tehrantimes.com/rss"),
        ("Iran", "https://iranprimer.usip.org/rss.xml"),
        ("Iran", "https://feeds.iranherald.com/rss/1b76a2b4cf7810bd"),
        ("Pakistan", "https://www.dawn.com/feeds/home"),
        ("Pakistan", "https://www.urdupoint.com/en/sitemap/news.rss"),
        ("Afghanistan", "https://feeds.afghanistannews.net/rss/6e1d5c8e1f98f17c"),
        ("Afghanistan", "https://pajhwok.com/feed/"),
        ("Afghanistan", "https://www.aopnews.com/feed/"),
        ("Turkmenistan", "https://www.hronikatm.com/feed/"),
        ("Turkmenistan", "https://feeds.turkmenistannews.net/rss/929bcf2071e81801"),
        ("Turkmenistan", "https://feeds.feedburner.com/NewsCentralAsia"),
        ("Turkmenistan", "https://orient.tm/feed/"),
        ("Uzbekistan", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/uzbekistan/rss.xml"),
        ("Kazakhstan", "https://astanatimes.com/feed/atom/"),
        ("Kazakhstan", "https://www.inform.kz/rss/eng.xml"),
        ("Kazakhstan", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/kazakhstan/rss.xml"),
        ("China", "https://chinadigitaltimes.net/feed/"),
        ("China", "https://technode.com/feed/"),
        ("China", "https://feeds.bbci.co.uk/news/world/asia/china/rss.xml"),
        ("China", "https://thediplomat.com/category/china-power/feed/"),
        ("China", "https://china-environment-news.net/feed/"),
        ("China", "https://www.nytimes.com/svc/collections/v1/publish/http://www.nytimes.com/topic/destination/china/rss.xml"),
        ("China", "https://feeds.beijingbulletin.com/rss/55582c89cb296d4c"),
        ("China", "https://www.scmp.com/rss/91/feed"),
        ("North Korea", "https://feeds.northkoreatimes.com/rss/08aysdf7tga9s7f7"),
        ("South Korea", "https://www.koreatimes.co.kr/www/rss/rss.xml"),
        ("South Korea", "https://en.yna.co.kr/RSS/news.xml"),
        ("South Korea", "https://english.hani.co.kr/rss/english_edition"),
        ("Japan", "https://www.japantimes.co.jp/feed/"),
        ("Japan", "https://japantoday.com/feed/atom"),
        ("Japan", "https://newsonjapan.com/rss/top.xml"),
        ("Japan", "https://english.kyodonews.net/rss/all.xml"),
        ("Japan", "https://www.nytimes.com/svc/collections/v1/publish/http://www.nytimes.com/topic/destination/japan/rss.xml"),
        ("Japan", "https://feeds.thejapannews.net/rss/c4f2dd8ca8c78044"),
        ("Taiwan", "https://feeds.feedburner.com/rsscna/engnews"),
        ("Taiwan", "https://api.taiwantoday.tw/en/rss.php?unit=2,6,10,15,18"),
        ("Indonesia", "https://en.antaranews.com/rss/news.xml"),
        ("Indonesia", "https://feeds.indonesianews.net/rss/f9295dc05093c851"),
        ("Indonesia", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/indonesia/rss.xml"),
        ("Indonesia", "https://www.ft.com/indonesia?format=rss"),
        ("Indonesia", "https://www.sindonews.com/feed"),
        ("Indonesia", "https://wartakota.tribunnews.com/rss"),
        ("Indonesia", "https://www.jpnn.com/index.php?mib=rss"),
        ("Indonesia", "https://rss.tempo.co/"),
        ("Indonesia", "https://www.cnnindonesia.com/rss"),
        ("Singapore", "https://theindependent.sg/feed/"),
        ("Singapore", "https://feeds.feedburner.com/hardwarezone/all"),
        ("Singapore", "https://feeds.singaporestar.com/rss/a677a0718b69db72"),
        ("Singapore", "http://feeds.singaporenews.net/rss/a677a0718b69db72"),
        ("Malaysia", "https://www.malaymail.com/feed/rss/malaysia"),
        ("Malaysia", "https://www.utusan.com.my/feed/"),
        ("Malaysia", "https://www.themalaysianinsight.com/feed"),
        ("Malaysia", "https://www.bernama.com/en/rssfeed.php"),
        ("Malaysia", "https://www.sarawakreport.org/feed/"),
        ("Papua New Guinea", "https://news.pngfacts.com/feeds/posts/default?alt=rss"),
        ("Australia", "https://www.abc.net.au/news/feed/2942460/rss.xml"),
        ("Australia", "https://www.9news.com.au/rss"),
        ("Australia", "https://www.sbs.com.au/feed/news/content-collection-rss/top-stories"),
        ("Australia", "https://feeds.feedburner.com/IndependentAustralia"),
        ("Australia", "https://www.smh.com.au/rss/world.xml"),
        ("Australia", "https://www.sbs.com.au/news/feed"),
        ("Australia", "https://www.abc.net.au/news/feed/51120/rss.xml"),
        ("New Zealand", "https://www.stuff.co.nz/rss"),
        ("New Zealand", "https://www.rnz.co.nz/rss/national.xml"),
        ("New Zealand", "https://thedailyblog.co.nz/category/daily_blogs/feed/"),
        ("Fiji", "https://fijisun.com.fj/feed/"),
        ("Fiji", "https://feeds.thefijinews.net/rss/7b9fd5fd3be1c082"),
        ("Fiji", "https://nzfijitimes.co.nz/feed/"),
        ("Madagascar", "https://www.madagascar-tribune.com/spip.php?page=backend"),
        ("Madagascar", "https://newsmada.com/feed/"),
        ("Thailand", "https://e.vnexpress.net/rss/news.rss"),
        ("Thailand", "https://www.khaosodenglish.com/feed/"),
        ("Thailand", "https://prachataienglish.com/rss.xml"),
        ("Myanmar(Burma)", "https://www.frontiermyanmar.net/en/feed/"),
        ("Myanmar(Burma)", "https://kachinnews.com/feed/?doing_wp_cron=1740036499.2181890010833740234375"),
        ("Bangladesh", "https://www.jagonews24.com/rss/rss.xml"),
        ("Nepal", "https://www.onlinekhabar.com/feed"),
        ("Nepal", "https://rajdhanidaily.com/feed/"),
        ("Morocco", "https://www.oujdacity.net/feed"),
        ("Morocco", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/morocco/rss.xml"),
        ("Mauritania", "https://lecalame.info/?q=rss.xml"),
        ("Mali", "https://bamada.net/feed"),
        ("Sudan", "https://www.altaghyeer.info/en/feed/"),
        ("Sudan", "https://www.dabangasudan.org/en/feed"),
        ("Sudan", "http://feeds.sudannews.net/rss/c1ab2109a5bf37ec"),
        ("Somalia", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/topic/destination/somalia/rss.xml"),
        ("Somalia", "https://sonna.so/en/feed/"),
        ("Somalia", "https://www.theguardian.com/world/somalia/rss"),
        ("Ethiopia", "https://thehabesha.com/feed/"),
        ("Ethiopia", "http://www.tadias.com/feed/atom/"),
        ("Kenya", "https://www.kenyans.co.ke/feeds/news?_wrapper_format=html"),
        ("Kenya", "https://www.businessdailyafrica.com/service/rss/bd/1939132/feed.rss"),
        ("Kenya", "https://www.theeastafrican.co.ke/service/rss/tea/1289142/feed.rss"),
        ("Kenya", "https://www.kenyanwhispers.co.ke/feed/"),
        ("Uganda", "https://www.4x4uganda.com/feed"),
        ("Uganda", "https://anchor.fm/s/5a74f2e8/podcast/rss"),
        ("Uganda", "https://exclusive.co.ug/feed/"),
        ("Uganda", "https://www.dignited.com/feed/"),
        ("Uganda", "https://scribe.co.ug/feed/"),
        ("Democratic Republic of Congo", "https://capsud.net/feed/"),
        ("Rwanda", "https://taarifa.rw/feed/"),
        ("Burundi", "https://burundi-eco.com/feed/"),
        ("Tanzania", "https://www.mwananchi.co.tz/service/rss/mw/2733734/feed.rss"),
        ("Tanzania", "https://mtanzania.co.tz/feed/"),
        ("Tanzania", "https://www.tanzaniainvest.com/feed"),
        ("Tanzania", "https://www.thecitizen.co.tz/service/rss/tanzania/2486554/feed.rss"),
        ("Angola", "https://www.club-k.net/index.php?option=com_obrss&task=feed&id=2:rss-noticias-do-club-k&format=feed&lang=pt"),
        ("Angola", "https://www.opais.ao/feed/"),
        ("Angola", "https://jornalf8.net/feed/"),
        ("Zambia", "https://www.times.co.zm/?feed=rss2"),
        ("Zambia", "https://lusakastar.com/feed"),
        ("Zambia", "https://lusakavoice.com/feed/"),
        ("Zambia", "https://www.mwebantu.com/feed/"),
        ("Zambia", "https://diggers.news/feed/"),
        ("Zambia", "https://zambianews365.com/feed/"),
        ("Malawi", "https://www.faceofmalawi.com/feed/"),
        ("Malawi", "https://www.maraviexpress.com/feed/"),
        ("Malawi", "https://times.mw/feed/"),
        ("Malawi", "https://malawi24.com/feed/"),
        ("Malawi", "https://www.maravipost.com/feed/"),
        ("Malawi", "https://malawifreedomnetwork.com/feed/"),
        ("Mozambique", "https://www.jornaldomingo.co.mz/feed/"),
        ("Zimbabwe", "https://www.chronicle.co.zw/feed/"),
        ("Zimbabwe", "https://www.sundaymail.co.zw/feed"),
        ("Zimbabwe", "https://iharare.com/feed/"),
        ("Zimbabwe", "https://mbaretimes.com/feed/"),
        ("Zimbabwe", "https://www.myzimbabwe.co.zw/feed"),
        ("Botswana", "https://botswanaunplugged.com/feed/"),
        ("Botswana", "https://news.thevoicebw.com/feed/"),
        ("Botswana", "https://guardiansun.co.bw/rssFeed/48"),
        ("Namibia", "https://www.namibian.com.na/feed/"),
        ("Namibia", "https://economist.com.na/feed/"),
        ("Namibia", "https://www.observer24.com.na/feed/"),
        ("Namibia", "https://namibiadailynews.info/feed/"),
        ("South Africa", "https://feeds.capi24.com/v1/Search/articles/News24/TopStories/rss"),
        ("South Africa", "https://www.dailymaverick.co.za/dmrss/"),
        ("South Africa", "https://www.sowetanlive.co.za/rss/?publication=sowetan-live"),
        ("South Africa", "https://rss.iol.io/iol/news"),
        ("South Africa", "https://www.citizen.co.za/feed/"),
        ("South Africa", "https://businesstech.co.za/news/feed/"),
        ("South Africa", "https://www.thesouthafrican.com/feed/"),
        ("South Africa", "https://www.africanews.com/feed/rss?themes=news"),
        ("South Africa", "https://www.timeslive.co.za/rss/"),
        ("South Africa", "https://www.businesslive.co.za/rss/"),
        ("Nigeria", "https://www.vanguardngr.com/feed/"),
        ("Nigeria", "https://guardian.ng/feed/"),
        ("Nigeria", "https://www.premiumtimesng.com/feed"),
        ("Nigeria", "https://rss.punchng.com/v1/category/latest_news"),
        ("Nigeria", "https://pmnewsnigeria.com/feed/"),
        ("Nigeria", "https://dailypost.ng/feed/"),
        ("Nigeria", "https://www.channelstv.com/feed/"),
        ("Nigeria", "https://punchng.com/feed/"),
        ("Ghana", "https://www.pulse.com.gh/rss"),
        ("Ghana", "https://ghheadlines.com/rss"),
        ("Ghana", "https://theheraldghana.com/feed/"),
        ("Guinea", "https://guineematin.com/feed/"),
        ("Guinea", "https://guineelive.com/feed/"),
        ("Guinea", "https://www.lejourguinee.com/feed/"),
        ("Guinea", "https://aminata.com/feed/"),
        ("World", "https://feeds.content.dowjones.io/public/rss/RSSWorldNews"),
        ("US", "https://feeds.content.dowjones.io/public/rss/RSSUSnews"),
        ("US", "https://feeds.content.dowjones.io/public/rss/socialpoliticsfeed"),
        ("World", "http://www.xinhuanet.com/english/rss/worldrss.xml"),
        ("China", "http://www.xinhuanet.com/english/rss/chinarss.xml"),
        ("Japan", "https://www3.nhk.or.jp/rss/news/cat0.xml"),
        ("India", "https://feeds.feedburner.com/NDTV-LatestNews"),
        ("India", "https://www.republicworld.com/rss/india.xml"),
        ("World", "https://en.yna.co.kr/RSS/news.xml"),
        ("South Korea", "https://www.yna.co.kr/RSS/national.xml"),
        ("North Korea", "https://www.yna.co.kr/RSS/nk.xml"),
        ("Canada", "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/canada/"),
        ("World", "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/world/"),
        ("Israel", "https://rss.jpost.com/rss/rssnewera"),
        ("Israel", "https://rss.jpost.com/rss/rssnews-updates"),
        ("Israel", "https://rss.jpost.com/rss/rssfeedsisraelnews.aspx"),
        ("Israel", "https://rss.jpost.com/rss/rssfeedsinternational"),
        ("Israel", "https://www.timesofisrael.com/feed/"),
        ("World", "https://www.tasnimnews.com/en/rss/feed/0/7/1194/world"),
        ("Iran", "https://www.tasnimnews.com/en/rss/feed/0/7/0/all-stories"),
        ("Pakistan", "https://feeds.feedburner.com/geo/GiKR"),
        ("Pakistan", "https://feeds.feedburner.com/geo/wUIl"),
        ("Egypt", "https://www.dailynewsegypt.com/feed/"),
        ("South Africa", "https://mg.co.za/feed/"),
        ("International", "https://www.lavanguardia.com/rss/internacional.xml"),
        ("Spain", "https://www.lavanguardia.com/rss/home.xml"),
        ("Vietnam", "https://tuoitre.vn/rss/tin-moi-nhat.rss"),
        ("Vietnam", "https://danviet.vn/rss/home.rss"),
        ("World", "https://www.bangkokpost.com/rss/data/world.xml"),
        ("Thailand", "https://www.bangkokpost.com/rss/data/topstories.xml"),
        ("Thailand", "https://www.bangkokpost.com/rss/data/thailand.xml"),
        ("Switzerland", "https://www.newlyswissed.com/feed/"),
        ("Switzerland", "https://lenews.ch/feed/"),
        ("Norway", "https://www.nrk.no/norge/toppsaker.rss"),
        ("Greece", "https://feeds.feedburner.com/kathimerini/DJpy"),
        ("Singapore", "https://www.straitstimes.com/news/singapore/rss.xml"),
        ("Singapore", "https://www.straitstimes.com/news/world/rss.xml"),
        ("Belgium", "https://www.standaard.be/rss/section/1f2838d4-99ea-49f0-9102-138784c7ea7c"),
        ("Belgium", "https://www.lesoir.be/rss2/2/cible_principale"),
        ("Portugal", "https://feeds.feedburner.com/publicoRSS"),
        ("Ethiopia", "https://addisstandard.com/feed/"),
        ("Venezuela", "https://www.elnacional.com/feed/"),
        ("Venezuela", "https://ultimasnoticias.com.ve/feed/"),
        ("UK", "https://www.telegraph.co.uk/rss.xml"),
        ("World", "https://rss.dw.com/rdf/rss-en-world"),
        ("Germany", "https://rss.dw.com/rdf/rss-en-ger"),
        ("Germany", "https://rss.dw.com/rdf/rss-en-top")
    ]

        # ─── 4. HELPER: REGION LOOKUP FOR EACH FEED ────────────────────────────────────
    feed_url_to_region = {feed_url: region for region, feed_url in data}

    # ─── 5. DATETIME UTILS ─────────────────────────────────────────────────────────
    tz_ist = pytz.timezone('Asia/Kolkata')
    def utc_to_ist(dt_utc):
        """Convert aware-UTC datetime → aware IST datetime."""
        return dt_utc.astimezone(tz_ist)

    # ─── 6. ARTICLE SCRAPER ────────────────────────────────────────────────────────
    def fetch_rss_articles(feed_url):
        """
        Parse one RSS feed → list of tuples ready for DataFrame.
        Keeps only items: (a) dated *today* in IST and (b) within last 30 min.
        """
        try:
            hdrs = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(feed_url, headers=hdrs, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "xml")

            region = feed_url_to_region.get(feed_url, "World")
            default_tz = pytz.timezone(REGION_TIMEZONES.get(region, "UTC"))

            now_ist = datetime.now(pytz.utc).astimezone(tz_ist)
            cutoff = now_ist - timedelta(minutes=30)
            today = now_ist.date()

            items = []
            for it in soup.find_all("item"):
                title = it.title.text.strip()           if it.title else "N/A"
                link  = it.link.text.strip()            if it.link  else "N/A"
                desc  = it.description.text.strip()     if it.description else ""
                author= it.author.text.strip()          if it.author else ""
                cat   = it.category.text.strip()        if it.category else ""

                pub_raw = it.pubDate.text.strip() if it.pubDate else ""
                pub_ist = None
                if pub_raw:
                    try:
                        tup = parsedate_tz(pub_raw)
                        if tup:
                            pub_utc = datetime.fromtimestamp(mktime_tz(tup), pytz.utc)
                        else:  # Fallback: assume default_tz
                            pub_naive = datetime.strptime(pub_raw, "%a, %d %b %Y %H:%M:%S")
                            pub_utc   = default_tz.localize(pub_naive).astimezone(pytz.utc)
                        pub_ist = utc_to_ist(pub_utc)
                    except Exception as e:
                        pass

                if pub_ist and pub_ist.date() == today and pub_ist >= cutoff:
                    items.append((soup.title.text if soup.title else feed_url,
                                title, link, region, pub_raw,
                                pub_ist.strftime('%Y-%m-%d %H:%M:%S'),
                                desc, author, cat, feed_url))
            return items
        except Exception as e:
            print(f"[skip] {feed_url}: {e}")
            return []

    # ─── 7. FETCH ALL FEEDS (multi-threaded for speed) ────────────────────────────
    all_articles = []
    with ThreadPoolExecutor(max_workers=50) as pool:
        futures = {pool.submit(fetch_rss_articles, url): url for url in rss_feeds}
        for fut in as_completed(futures):
            all_articles.extend(fut.result())

    if not all_articles:
        raise SystemExit("No fresh items found in the last 30 minutes. Exiting.")

    df = pd.DataFrame(all_articles, columns=[
            "Source","Title","Link","Region","PublishedDate",
            "date_ist","Description","Author","Category","URL"
    ])

    # ─── 8. DATETIME COERCION & LAST-30-MIN FILTER (safety) ───────────────────────
    df["PublishedDate"] = pd.to_datetime(df["PublishedDate"], errors='coerce')
    df["date_ist"] = pd.to_datetime(df["date_ist"], errors="coerce")
    df["date_ist"] = df["date_ist"].apply(
        lambda ts: ts.tz_localize("Asia/Kolkata") if pd.notnull(ts) and ts.tzinfo is None
        else ts.tz_convert("Asia/Kolkata") if pd.notnull(ts) else ts
    )

    # 2️⃣ cutoff timestamp (already IST-aware)
    cutoff_pd = pd.Timestamp.now(tz="Asia/Kolkata") - pd.Timedelta(minutes=30)

    # ✅ time-window filter – now both operands are tz-aware
    df = df[df["date_ist"] >= cutoff_pd]
    # ─── 9. DEDUPLICATE BASIC (title + description + URL) ─────────────────────────
    df = df.drop_duplicates(subset=["Title","Description"]).reset_index(drop=True)

    print(f"✔️  Fresh, unique articles: {len(df)}")

    NA_RE  = re.compile(r"\[?\s*(?:n/?a|none|null|nan)\s*\]?", re.I)
    BR_RE  = re.compile(r'[\[\]"\{\}]')            # brackets & quotes
    SC_RE  = re.compile(r"[^\w\s\-',&/\.]")        # strip weird specials

    # columns that exist **already** before extraction
    RAW_COLS = ["Title", "Description", "Author", "Category"]

    def _clean_cell(val: str) -> str:
        if val is None or NA_RE.fullmatch(str(val)):
            return ""
        return BR_RE.sub("", str(val)).strip()

    def basic_hygiene(df: pd.DataFrame):
        for c in RAW_COLS:
            if c in df.columns:
                df[c] = df[c].map(_clean_cell)
            return df

    df = basic_hygiene(df)
    # ─── 10. ROOT-URL COLUMN + EXTRACTION HELPERS ─────────────────────────────────
    def extract_root(url):
        try:
            p = urlparse(url)
            return f"{p.scheme}://{p.netloc}/"
        except: return None
    df["root_url"] = df["Link"].map(extract_root)

    # ---------- Content-extraction utilities ----------
    def wn_link(page_url, max_retries=20, delay=3):
        """
        For article.wn.com pages: digs the first real outbound <a>.
        Returns original URL if none found.
        """
        for attempt in range(max_retries):
            try:
                html = requests.get(page_url, timeout=10).text
                soup = BeautifulSoup(html,"html.parser")
                main = soup.find('div', class_='main-article')
                if main:
                    for a in main.find_all('a', href=True):
                        if a['href'].startswith("http"):
                            return a['href']
                return page_url
            except Exception:
                sleep(delay)
        return page_url

    def download_html(url, timeout=15):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception: return None

    def extract_article(url, html_cache={}):
        if url not in html_cache:
            html_cache[url] = download_html(url)
        html = html_cache[url]
        if not html: return None
        try:
            art = Article(url)
            art.set_html(html); art.parse()
            return art.text.strip()
        except Exception:
            return None

    # Diffbot fallback -------------------------------------------------------------
    DIFFBOT_TOKEN = os.environ["DIFFBOT_TOKEN"]
    def diffbot_extract(url, retries=3):
        api = f"https://api.diffbot.com/v3/article?url={url}&token={DIFFBOT_TOKEN}&timeout=60000"
        for _ in range(retries):
            try:
                j = requests.get(api, timeout=20).json()
                return j.get("objects",[{}])[0].get("text")
            except Exception:
                sleep(5)
        return None

    # Root-specific extractors (inkl, headtopics) ----------------------------------
    def inkl_extract(url):
        try:
            soup = BeautifulSoup(download_html(url),"html.parser")
            art  = soup.find('div', class_='article-content')
            return "\n".join(p.text for p in art.find_all('p')) if art else None
        except Exception: return None

    def headtopics_extract(url):
        try:
            soup = BeautifulSoup(download_html(url),"html.parser")
            sec  = soup.find('section', class_='Article-contentBlocks')
            text = " ".join(p.text for p in sec.find_all('p')) if sec else ""
            idx  = text.find("Source:")
            return text[:idx].strip() if idx!=-1 else text
        except Exception: return None

    # ─── 11. CONTENT EXTRACTION PIPELINE ──────────────────────────────────────────
    def pipeline(row):
        url = row["Link"]

        # article.wn.com fix
        if row["root_url"] == "https://article.wn.com/":
            url = wn_link(url)

        # primary extraction
        txt = extract_article(url)

        # inkl special-case
        if row["root_url"] == "https://www.inkl.com/":
            txt = inkl_extract(url) or txt

        # headtopics special-case
        if row["root_url"] == "https://headtopics.com/":
            txt = headtopics_extract(url) or txt

        # fallback via Diffbot if still None/short
        if not txt or len(txt)<200:
            txt = diffbot_extract(url) or txt

        # final fallback: use Description if still empty
        if not txt:
            txt = row["Description"]

        return pd.Series({"URL_final": url, "Article_Content": txt})

    print("🔍 Extracting article bodies (this may take a while)…")
    with ThreadPoolExecutor(max_workers=50) as pool:
        parts = list(pool.map(pipeline, [r[1] for r in df.iterrows()]))
    df_parts = pd.DataFrame(parts)
    df = pd.concat([df.reset_index(drop=True), df_parts], axis=1)

    # Drop duplicates on cleaned URL + content
    df = df.drop_duplicates(subset=["URL_final","Article_Content"]).reset_index(drop=True)

    def fallback_body(df: pd.DataFrame):
        """
        If Article_Content is empty/NaN/very short, fall back to Description
        (still useful for headlines flashes).
        """
        mask = df["Article_Content"].fillna("").str.len() < 50
        df.loc[mask, "Article_Content"] = df.loc[mask, "Description"].fillna("")
        return df
    df = fallback_body(df)


    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    OPENAI_MODEL = "gpt-4o-mini"
    client = OpenAI(api_key=OPENAI_API_KEY)

    # Keep your existing system prompt exactly as before:
    SYSTEM_MSG = (
        "You are a risk-intelligence analyst for enterprise risk management. "
        "Extract and classify entities from the text for a comprehensive risk-intelligence analysis."
    )

    # These are the 20 fields you want back:
    FIELD_LIST = [
        "Topic","Subtopic","Persons","Organizations","Locations","Date/Time",
        "Event/Incident","Object/Product","Legal/Document","Impact/Response",
        "Sentiment/Motivation","Threat Type","Severity/Magnitude","Impact Assessment",
        "Primary_Location","Primary_Latitude","Primary_Longitude","Country",
        "Keywords","Sentiment_score"
    ]

    # Build a JSON‐Schema that exactly mirrors FIELD_LIST:
    SCHEMA = {
        "type": "object",
        "properties": {
            fld: {"type": "string", "description": fld}
            for fld in FIELD_LIST
        },
        "required": FIELD_LIST,
        "additionalProperties": False
    }



    # ─── 2.  SINGLE‐ARTICLE CALL WITH STRUCTURED OUTPUT ───────────────────────────

    def call_openai_structured(article: str,
                            max_retries: int = 3,
                            backoff_sec:   int = 10):
        """
        Sends your prompt + Article_Content to OpenAI *forcing* it to return a JSON object
        matching SCHEMA.  Returns a Python dict with one entry per FIELD_LIST.
        """
        user_prompt = f"""
        Classify the following text into one of the following risk topics: Crime, Disaster, Economy, Education, Environment,
        Health, Legal, Military, Political, Security, Social, Sport, Technology, Terrorism.
        Also, provide a relevant subcategory that is universally applicable.
        Extract the following entities from the text and provide their details:
        1. Persons: all the person entities present in the article
        2. Organizations: all the organisation names( entities present in the article)
        3. Locations: all the locations in focus in the article
        4. Date/Time: the date and time of events mentioned specifically in the article
        5. Event/Incident: 
        6. Object/Product
        7. Legal/Document
        8. Impact/Response
        9. Sentiment/Motivation
        10. Threat Type
        11. Severity/Magnitude
        12. Impact Assessment
        13. Primary_Location: Give only one primary location from Article_Content.
        14. Primary_Latitude: Give the latitude of Primary_Location you extracted.
        15. Primary_Longitude: Give the longitude of Primary_Location you extracted.
        16. Country: Give the Country name in which Primary_Location lies.
        17. Keywords: Give keywords that help define the article (could be words that are not present in the article also but perfectly fall in the bracket of keywords for the article)
        18. Sentiment_score: Give the sentiment score of the article in the range of -1 to 1, where -1 is very negative, 0 is neutral, and 1 is very positive.
        19. Topic: Give the category of the article only from the list of categories provided:['Crime', 'Disaster', 'Economy', 'Education', 'Environment', 'Health', 'Legal', 'Military', 'Political', 'Security', 'Social', 'Sport', 'Technology', 'Terrorism']
        20. Subtopic: Give the subcategory of the article based on the topic category.
        If something is not applicable or not defined or N/A. Please leave the cell as blank.

        user:
        Article Content: {article}
        assistant:
    """

        for attempt in range(1, max_retries + 1):
            try:
                resp = client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_MSG},
                        {"role": "user",   "content": user_prompt}
                    ],
                    response_format={
                        "type": "json_schema",
                        "json_schema": {
                            "name": "rss_enrichment",
                            "strict": True,
                            "schema": SCHEMA
                        }
                    }
                )
                    # content is guaranteed JSON string matching SCHEMA
                json_str = resp.choices[0].message.content
                return json.loads(json_str)

            except Exception as e:
                if attempt == max_retries:
                    # return empty defaults on final failure
                    return {fld: "" for fld in FIELD_LIST}
                time.sleep(backoff_sec * attempt)

        # unreachable
        return {fld: "" for fld in FIELD_LIST}


    # ─── 3.  ENRICH DATAFRAME IN PARALLEL ────────────────────────────────────────

    def enrich_dataframe(df: pd.DataFrame, workers: int = 4) -> pd.DataFrame:
        """
        Applies call_openai_structured() to each Article_Content in parallel,
        returns a new DataFrame with one column per FIELD_LIST.
        """
        results = [None] * len(df)

        def task(i, txt):
            results[i] = call_openai_structured(txt or "")

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(task, idx, text)
                for idx, text in enumerate(df["Article_Content"].fillna(""))
            ]
            for _ in as_completed(futures):
                pass

        enrich_df = pd.DataFrame(results).fillna("")
        # ensure columns exist and are in the right order
        for fld in FIELD_LIST:
            if fld not in enrich_df:
                enrich_df[fld] = ""
        enrich_df = enrich_df[FIELD_LIST]

        return pd.concat([df.reset_index(drop=True), enrich_df], axis=1)

    # compile once, reuse
    NA_RE = re.compile(r"^(?i:n/?a|none|null|\s*)$")   # empty / N/A-ish tokens
    BR_RE = re.compile(r"[\[\]\{\}\"'<>]")             # stray brackets & quotes

    def _clean_series(s: pd.Series | pd.DataFrame) -> pd.Series:
        """
        Accepts either a Series or a single-column DataFrame and returns
        a cleaned Series.  If a DataFrame sneaks in, we grab the first column.
        """
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]

        s = s.astype(str)
        s = s.str.replace(NA_RE, "", regex=True)
        s = s.str.replace(BR_RE, "", regex=True)
        s = s.str.strip()
        return s.mask(s == "", "Other")


    def post_enrichment_clean(df: pd.DataFrame) -> pd.DataFrame:
        tgt_cols = ["Topic","Subtopic","Persons","Organizations","Locations",
                    "Event/Incident","Object/Product","Legal/Document","Impact/Response",
                    "Sentiment/Motivation","Threat Type","Severity/Magnitude",
                    "Impact Assessment","Primary_Location","Country","Keywords"]

        for c in tgt_cols:
            if c in df.columns:
                df[c] = _clean_series(df[c])

        # ––– country harmonisation & capitalisation ––––––––––––––––––––
        if "Country" in df.columns:
            df["Country"] = (df["Country"]
                            .str.title()
                            .replace({"Usa": "United States",
                                        "Other": "Incognito"}))

        cap_cols = ["Topic","Subtopic","Country","Sentiment/Motivation"]
        for c in cap_cols:
            if c in df.columns:
                df[c] = df[c].str.title()

        return df




    # ─── 4.  RUN & SAVE ──────────────────────────────────────────────────────────

    df_enriched = enrich_dataframe(df, workers=2)

    df_enriched = post_enrichment_clean(df_enriched)
    def final_rename(df: pd.DataFrame):
        """
        Replace any illegal chars (Excel & SQL hate them), esp. '/'.
        """
        df.columns = [c.replace("/", "_").strip() for c in df.columns]
        return df


    # strip tz from dates if needed
    for col in ("date_ist", "PublishedDate"):
        if col in df_enriched:
            df_enriched[col] = (
                pd.to_datetime(df_enriched[col], errors="coerce")
                .dt.tz_localize(None)
            )
    df_enriched['headline'] = df_enriched['Title'].str.strip()
    df_enriched['general_category'] = df_enriched['Category'].str.strip()
    df.drop(columns=["Title", "Category"], inplace=True, errors='ignore')
    df_enriched["id"] = np.arange(len(df_enriched))
    df_enriched = final_rename(df_enriched)
    def extract_time_for_search():
        # Setup the IST timezone
        ist = pytz.timezone('Asia/Kolkata')
    
        # Get the current date and time in IST
        ist_time = datetime.now(ist)
    
        # Format the datetime object to match the desired output format
        formatted_time = ist_time.strftime("%d-%b-%Y %I:%M%p")  # Format the time
        formatted_time = formatted_time.replace(ist_time.strftime("%d"), str(int(ist_time.strftime("%d"))))  # Remove leading zero
    
        return formatted_time

    # Define the function to make OpenAI API calls

    def transform_query(row):

        article_headline = row['headline']
        article_summary = row['Article_Content']
        article_date = extract_time_for_search()

        prompt = f"""
    **Objective**:
    1. A newly posted data point (article posted within the last 30 minutes) will be provided. This data must be broken down into different smaller queries, each designed to extract relevant context or longitudinal insights to provide a deeper understanding of the event or topic.
    2. The goal is to generate multiple focused queries that, when aggregated, will offer a well-rounded and comprehensive view of the event, its context, potential future implications, and related past occurrences.
    3. Each query should aim to provide additional layers of analysis, including but not limited to: the event itself, historical or trend data, demographic information, regional comparisons, expert opinions, and longitudinal impact.
    4. the main goal of these queries is to generate all the angles that can be fed into a search engine that would retrieve results that would help in generating a an alert for the event that would be based off of longitudinal analysis of the event and cover the topic with a broader scope  by a risk analyst 

    **Guidelines**:
    1. **Atomic Queries**: Break down the main data point into specific atomic queries. Each query should address a different aspect of the event to ensure diverse data retrieval.
    2. **Event-Specific**: Queries should focus directly on the core of the event, extracting immediate, relevant information.
    3. **Contextual**: Retrieve broader context about the event, such as related events, social conditions, government policies, or reactions in the same region or sector.
    4. **Demographic Impact**: Queries should seek to explore the demographic impact, including groups affected by or involved in the event, their characteristics, and their historical trends in similar situations.
    5. **Historical/Trend-Based**: Aim to generate queries that compare the current event with past occurrences in the same region or related events over time. Look for historical insights or patterns that can help identify trends.
    6. **Secondary Sources**: Generate queries aimed at discovering new commentary or real-time data points that may not be present in the initial article (e.g., social media reactions, expert analysis, wire reports).
    7. **Dynamic Event Monitoring**: Incorporate queries that allow for tracking the potential future developments of the event, considering its fluidity over time.

    **Considerations for Queries**:
    - Focus on different angles (event-specific, contextual, demographic, and trend-based) so that the search engine retrievals provide a broad perspective.
    - Each query should be concise (less than 15 words) and self-contained.
    - Include proper names, places, and factual data directly from the data point without using vague pronouns (like “he” or “it”).
    - Ensure queries reflect the event’s real-time nature (considering the event was reported in the last 30 minutes).
    - Queries should also look for related expert opinions, social media discussions, and real-time updates.
    - Reference the current date and time when generating queries to determine if the queries indicate 
    occurrences of events or concepts in the future or past.
    -Optimise the queries to be used in a search engine.
    -Do not exclude any facts, figures, proper nouns or time-related information present in the user query, from the generated queries.
    - Based on the 7 points mentioned in the guidelines , generate upto 6 queries maximum  that would satisfy all the conditions required to get data for a complete analysis on the event

    [Examples]:

    user:

    current datetime : 27-Aug-2024 02:55PM
    Article headline: West Bengal: Police arrest 4 students ahead of ‘Nabanna Abhijan’ protest march claiming they were ‘planning large-scale violence’, BJP to move court
    Article summary : Tensions are high in West Bengal as the state government, led by Mamata Banerjee, opposes the "Nabanna Abhijan" rally, organized to protest the rape and murder of a doctor at RG Kar Medical College and Hospital. On August 27th, Leader of the Opposition Suvendu Adhikari accused the government of detaining four students—Subhojit Ghosh, Pulokesh Pandit, Goutam Senapati, and Pritam Sarkar—who were set to join the protest. Adhikari claimed the students were missing and unresponsive, suggesting they had been arrested by the police.
    The West Bengal Police refuted these claims, stating the students were arrested for allegedly planning large-scale violence during the event and were involved in a conspiracy of murder and attempted murder. The police emphasized that the arrests were made in the interest of public safety and that the students' families had been informed.
    In response, Adhikari announced that the families of the arrested students had approached the Calcutta High Court. The ruling Trinamool Congress (TMC) argued that the protest was politically motivated and opposed it, while the Bharatiya Janata Party (BJP) accused TMC of suppressing dissent. Extensive security measures have been implemented for the protest march to the West Bengal state secretariat.
    The student organization "Paschimbanga Chhatro Samaj" maintained that their rally would be peaceful and aimed at demanding Mamata Banerjee's resignation and the arrest of those responsible for the doctor's death. However, on August 26th, Additional Director General of Police (South Bengal) Supratim Sarkar announced that the group's application to hold the march had been denied, labeling the agitation as illegal and intended to incite unrest. 

    assisstant: 
    ["Details of the arrest of Subhojit Ghosh, Pulokesh Pandit, Goutam Senapati, and Pritam Sarkar on August 27, 2024",
    "Previous instances of protests in West Bengal under Mamata Banerjee's government", "Impact of the 'Nabanna Abhijan' protest on student communities in West Bengal",
    "Historical analysis of political protests in West Bengal and their outcomes", "Social media reactions to the arrest of students ahead of 'Nabanna Abhijan' protest",
    "Future developments and court proceedings related to the arrest of students in West Bengal protest"]

    user:

    current datetime : 27-Aug-2024 04:02PM
    Article headline: Electric vehicle tariffs spark EU-China trade war fears (Atradius NV)
    Article summary : The European Union's decision to impose new tariffs on Chinese-made electric vehicles (EVs) by the end of October, pending member state approval, underscores broader Western concerns about aggressive Chinese pricing and its impact on local industries. The recalculated tariffs, which are lower than provisional rates announced in June, aim to protect EU (European Union) manufacturers from what is perceived as unfairly subsidized Chinese imports. German brands like Volkswagen and BMW will face a 21.3% tariff, while Tesla benefits from a reduced 9% rate. Major Chinese EV (Electric Vehicle) makers like BYD, Geely, and SAIC also see slight reductions in their tariffs.
    The tariffs reflect the EU's case-by-case approach, contrasting with the US's blanket 100% tariff on Chinese EVs. The move highlights fears of a trade war, as tariffs could lead to retaliatory measures and disrupt global supply chains. The Chinese export surge is driven by weak domestic demand, government incentives, and China's cost advantage in EV manufacturing. Western producers, already strained by high input costs and inflation, are unprepared for the influx of cheaper Chinese imports.
    The tariffs may slow Chinese imports temporarily, giving European producers a chance to innovate, but could also lead to higher prices and reduced market competitiveness. The potential for a broader trade war looms, with China investigating European imports. The situation adds uncertainty to the EV sector, which is already facing challenges, and could hinder sustainability efforts by keeping prices high. European manufacturers oppose the tariffs, advocating for improved competitiveness through innovation and value-added services. The EU remains open to negotiations with Beijing, hoping to avoid a damaging trade conflict.


    assisstant:
    ["Impact of EU tariffs on Chinese EV imports by end of October 2024", "Comparison of EU and US tariffs on Chinese electric vehicles",
    "Effect of new EU tariffs on German EV brands like Volkswagen and BMW", "Chinese government incentives driving EV export surge in 2024",
    "Potential retaliatory measures by China against EU tariffs on EVs", "Historical trends in EU-China trade relations and tariff disputes"]

    user:

    current datetime : 27-Aug-2024 02:55PM
    Article headline: Powerful 5.4 magnitude earthquake hits Portugal’s coastline
    Article summary : A powerful 5.4 magnitude earthquake struck southern Portugal early Monday morning at 5:11 a.m. local time, with its epicenter located approximately 58 km west of the coastal town of Sines, according to the United States Geological Survey (USGS). The tremors were felt intensely in Sines, Lisbon, and Setúbal, and reports of the shaking extended to Porto, Spain, and Morocco. Despite the widespread impact, no injuries were reported, and Portugal remained largely unscathed. The Institute of the Sea and Atmosphere (IPMA) confirmed at least four minor aftershocks followed the initial quake, though their intensity was minimal.

    assisstant:
    ["Details of the 5.4 magnitude earthquake in Portugal on 27-Aug-2024", "Historical earthquake data for Portugal’s coastline near Sines",
    "Impact of the 5.4 magnitude earthquake on Sines, Lisbon, and Setúbal", "Expert opinions on the 5.4 magnitude earthquake in Portugal on 27-Aug-2024",
    "Social media reactions to the Portugal earthquake on 27-Aug-2024", "Aftershocks following the 5.4 magnitude earthquake in Portugal on 27-Aug-2024"]

    user:

    current datetime : 27-Aug-2024 08:30PM
    Article headline: Troops Arrest Kidnap Kingpins, Terrorists Behind Killing of Taraba Traditional Ruler, Son
    Article summary : Troops of the 6 Brigade/Sector 3 Operation Whirl Stroke have successfully arrested several kidnappers, terrorists, and suspected bandits across various locations in Taraba State. Among those apprehended are individuals linked to the murder of a traditional ruler, Kumbiya Tanimu, and his son, Yusuf, in Jankindo village, Takum LGA. The arrests followed a series of clearance operations conducted in collaboration with other security agencies. Notably, on August 18, 2024, a raid in Takum LGA led to the arrest of a suspected kidnapper, David Musa, who is associated with a notorious kingpin known as 'Skin the Body.' Further operations on August 20 and 21 resulted in the capture of gunrunners and the Tiv militia kingpin, Mela Selim, believed to have orchestrated the killings. The troops also countered an ambush by terrorists in Ussa LGA, recovering weapons and motorcycles. The Nigerian Army has urged the public to continue providing intelligence to aid in maintaining peace in the State.

    assisstant:
    ["Details of the arrest of David Musa in Takum LGA on August 18, 2024", "Previous incidents of violence involving traditional rulers in Taraba State",
    "Impact of recent arrests on local communities in Takum and Ussa LGAs", "Historical data on kidnappings and terrorist activities in Taraba State",
    "Expert opinions on the effectiveness of Operation Whirl Stroke in Taraba State", "Updates on the situation in Taraba State following the arrests on August 20 and 21, 2024"]

    user:

    current datetime : 27-Aug-2024 09:03PM
    Article headline: Cargo plane makes emergency landing at Japan’s Narita airport
    Article summary : A Boeing 747 cargo plane operated by Atlas Air, flight 7106 bound for Los Angeles, made an emergency landing at Japan’s Narita International Airport early Tuesday morning due to a hydraulic system malfunction. The plane landed at around 1:10 a.m., during which a tyre burst and wheel damage were detected, leading to the closure of Runway A for approximately seven hours. Fortunately, none of the seven crew members on board were injured. Despite the runway closure, there were no significant delays or cancellations of other flights.

    assisstant:
    ["Atlas Air flight 7106 emergency landing details at Narita Airport", "Historical data on hydraulic system malfunctions in Boeing 747 cargo planes",
    "Impact of Narita Airport Runway A closure on flight schedules", "Expert opinions on hydraulic system failures in cargo planes",
    "Social media reactions to Atlas Air emergency landing on August 27, 2024", "Previous emergency landings at Narita International Airport and their outcomes"]

    user:

    current datetime : 27-Aug-2024 10:20PM
    Article headline: IMD (Indian Meteorological Department) Predicts Heavy Rains in Kerala; Issues Orange Alert
    Article summary : The India Meteorological Department (IMD) has issued an orange alert for Kerala, predicting very heavy rainfall ranging from 11 cm to 20 cm. Additionally, thunderstorms accompanied by light to moderate rainfall and gusty winds reaching speeds of 40 km/h are expected in several districts, including Thiruvananthapuram, Kollam, Pathanamthitta, Alappuzha, Kottayam, Idukki, Ernakulam, and Thrissur. These weather conditions are anticipated to occur in the coming hours.

    assisstant:
    ["Current weather conditions in Kerala districts Thiruvananthapuram, Kollam, Pathanamthitta, Alappuzha, Kottayam, Idukki, Ernakulam, Thrissur",
    "Historical rainfall data for Kerala during August in the past 10 years", "Impact of heavy rainfall on Kerala's infrastructure and agriculture",
    "Previous instances of orange alerts issued by IMD in Kerala and their outcomes", "Social media reactions to IMD's orange alert for Kerala on 27-Aug-2024",
    "Expert opinions on IMD's prediction of heavy rains in Kerala on 27-Aug-2024"]

    user:

    current datetime : 27-Aug-2024 11:00PM
    Article headline: Typhoon Wukong Moves Slowly Northeast from Miyako Island; Heavy Rainfall Expected in Japan’s Okinawa, Southern Kyushu Areas.
    Article summary : Typhoon Jongdari, also known as Typhoon No. 9, has formed approximately 70 kilometers south of Miyako Island in Okinawa Prefecture. The Japan Meteorological Agency reports that the typhoon is moving northeast at around 80 kilometers per hour, with a central pressure of 1,000 hectopascals. Heavy rainfall is anticipated on Okinawa’s main island, the Sakishima Islands, and southern Kyushu. Local meteorological observatories have issued advisories, warning of potential landslides, flooding in low-lying areas, and rising river levels. The typhoon is expected to continue its northward trajectory over the East China Sea through Tuesday.

    assisstant:
    ["Current impact of Typhoon Jongdari on Miyako Island and surrounding areas", "Historical typhoon patterns affecting Okinawa and southern Kyushu regions",
    "Demographic analysis of populations affected by Typhoon Jongdari in Okinawa", "Government advisories and emergency measures for Typhoon Jongdari in Japan",
    "Social media reactions to Typhoon Jongdari in Okinawa and southern Kyushu", "Expert opinions on the potential long-term impact of Typhoon Jongdari on Japan’s coastal regions"]

    [END OF EXAMPLES]
    user:

    current datetime : {article_date}
    Article headline: {article_headline}
    Article summary : {article_summary}

    assisstant:
    """

        # Call OpenAI's API to generate the response
        try:
            client = OpenAI(
                api_key= os.environ["OPENAI_API_KEY"],
            )

            chat_completion = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": "Embody the role of a seasoned AI assistant tasked with decomposing given news article data into atomic queries for efficient data retrieval. Each query must be concise, self-contained, and each designed to extract relevant context or longitudinal insights to provide a deeper understanding of the event or topic to ensure optimal search engine performance. Each query should reflect precise, time-specific data, avoiding vague references and ensuring clarity in all decompositions. Follow the given instructions strictly."},
                    {"role": "user", "content": prompt}
                ],
                model="gpt-4o-mini",
            )

            # Extract output from OpenAI's response
            gpt_output=chat_completion.choices[0].message.content
            return gpt_output
        except Exception as e:
            return str(e)
        
    df_enriched['transformed_qs'] = df_enriched.apply(transform_query,axis=1)
    def bing_query_creation(new_queries):
        
        def extract_keyphrase(single_intent):
        # Craft the prompt
            prompt = f"""Objective:
        Generate 1 key phrase from the input, optimizing it for Bing search. 
        You should identify and prioritize relevant terms or phrases that are likely to yield meaningful search results, 
        enhancing the discoverability of the content online. Consider factors such as relevance, uniqueness, 
        and conciseness in selecting the key phrases. Ensure that the extracted phrase accurately represent and 
        embodies the essence of the input line to enhance its searchability on Google.
        [EXAMPLES]
    
        user query:
        What are some effective strategies for managing stress and anxiety during the COVID-19 pandemic, especially when working remotely and feeling isolated from colleagues?
        assistant:
        Managing stress and anxiety while working remotely during COVID-19
        user query:
        I'm planning a trip to Japan and want to explore both traditional and modern attractions, including historical sites, vibrant city life, and culinary experiences. Can you recommend an itinerary that covers a mix of these elements?
        assistant:
        traditional and modern attractions in Japan: itinerary recommendations
        user query:
        How can I troubleshoot my Wi-Fi connection if it keeps dropping intermittently, and I've already tried restarting my router and updating the firmware, but the issue persists?
        assistant:
        Troubleshooting intermittent Wi-Fi connection drops
    
        user query:
        Reasons behind the coup in Bangladesh as of 6th August 2024
        assistant:
        Reasons for coup in Bangladesh as of 6th August 2024
        [END EXAMPLES]
    
        user:
        {single_intent}
        assistant:
    
        """
    
            client = OpenAI(
                api_key= os.environ["OPENAI_API_KEY"],
            )
    
            chat_completion = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": "Embody the role of a seasoned AI assistant tasked with transforming user queries into search engine optimised key phrases. Ensure that the meaning of the original query is retained by the generated key phrases, and they accurately represent the context and logic of the user query. Follow the given instructions strictly."},
                    {"role": "user", "content": prompt}
                ],
                model="gpt-4o-mini",
            )
    
            gpt_output=chat_completion.choices[0].message.content
    
            return gpt_output
    
        try:
            # Attempt to parse the JSON input
            query_list = json.loads(new_queries)
        except Exception as e:
            # Catch all possible exceptions that might occur during JSON parsing
            return []  # Return empty list if any error occurs
        # List to store the outputs
        outputs = []
    
        # Iterate over each query, apply the extract_keyphrase function, and append the result to outputs
        for query in query_list:
            try:
                keyphrase_output = extract_keyphrase(query)
                outputs.append(keyphrase_output)
            except Exception as e:
                # Optionally handle or log errors during the keyphrase extraction process
                outputs.append(None)  # Append None or a custom error message if processing fails
    
        return outputs

    df_enriched['keyphrases'] = df_enriched['transformed_qs'].apply(bing_query_creation)

    def single_bing_search(outputs):
        def search_bing(query):
            """
            This function makes an API call to the Bing Search API and returns the result in JSON format.
            :param query: The search query string.
            :return: A dictionary containing the search results.
            """
            def make_bing_api_call(query):
                # Define the API endpoint and parameters
                endpoint = "https://google.serper.dev/search"
                api_key = "6b7cb841647992df27b5457528554fadacac1e06"  # Replace with your actual Serper API key
                headers = {
                    "X-API-KEY": api_key,
                    "Content-Type": "application/json"
                }
                payload = json.dumps({
                    "q": query,
                    "num": 10
                })

                # Make the API call
                response = requests.post(endpoint, headers=headers, data=payload)
                print(f"Response Status Code: {response.status_code}")
                
                # If the response status code is 400, print the error message
                if response.status_code == 400:
                    print(f"Error 400: {response.text}")
                    return {}
                elif response.status_code == 200:
                    # Debug: print the raw response
                    print(f"Raw Response: {response.text}")
                    return response.json()
                else:
                    response.raise_for_status()

            # Call the Bing Search API with the provided query
            try:
                result = make_bing_api_call(query)
                return result
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                return {}

        # Combine all keyphrases into a single query string with "OR" between each phrase
        # Ensure the keyphrases are not broken down into individual characters
        combined_query = " OR ".join(outputs)

        # Ensure the query is not too long (max 2048 characters)
        if len(combined_query) > 2048:
            combined_query = combined_query[:2048]

        # Call Bing search API with the combined query
        bing_output = search_bing(combined_query)

        # Debug: print the final search output
        print(f"Search Output for '{combined_query}': {bing_output}")

        return bing_output

    df_enriched['serper_jsons_lists'] = df_enriched['keyphrases'].apply(single_bing_search)
    def parse_serper_results(serper_jsons_list):
        def extract_snippets_and_articles(results):
            snippets = []
            urls_to_download = []

            # Extracting from Google SERP structure
            if 'organic' in results:
                organic_results = results.get('organic', [])
                for result in organic_results:
                    title = result.get('title', '')
                    snippet = result.get('snippet', '')
                    url = result.get('link', '')  # 'link' instead of 'url' in Google SERP
                    formatted_result = f"{title}, {snippet}"
                    snippets.append(formatted_result)
                    urls_to_download.append(url)

            if 'news' in results:
                news_items = results.get('news', [])
                for news_item in news_items:
                    title = news_item.get('title', '')
                    snippet = news_item.get('snippet', '')
                    url = news_item.get('link', '')
                    formatted_result = f"{title}, {snippet}"
                    snippets.append(formatted_result)
                    urls_to_download.append(url)

            # Article downloading and parsing
            downloaded_articles = {}
            num_threads = max(1, min(10, len(urls_to_download)))  # Ensure at least 1 thread

            def download_article(url):
                try:
                    response = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
                    response.raise_for_status()
                    return response.text
                except requests.RequestException:
                    return None

            def extract_article_content(url):
                if url not in downloaded_articles:
                    html = download_article(url)
                    if html:
                        downloaded_articles[url] = html
                    else:
                        return None

                try:
                    article = Article(url)
                    article.set_html(downloaded_articles[url])
                    article.parse()
                    return article.text
                except Exception:
                    return None

            # **Prevent ThreadPoolExecutor from running if no URLs exist**
            if urls_to_download:
                with ThreadPoolExecutor(max_workers=num_threads) as executor:
                    futures = {executor.submit(extract_article_content, url): url for url in urls_to_download}
                    for future in as_completed(futures):
                        url = futures[future]
                        content = future.result()
                        if content:
                            for idx, snippet in enumerate(snippets):
                                if url in snippets[idx]:  # Associate content with correct snippet
                                    snippets[idx] += f", Article Content: {content[:500]}"  # Limit to 500 chars

            return snippets

        # Convert JSON string to dictionary if needed
        if isinstance(serper_jsons_list, str):
            try:
                serper_jsons_list = json.loads(serper_jsons_list)
            except json.JSONDecodeError:
                return []

        return extract_snippets_and_articles(serper_jsons_list)

    df_enriched['bing_final'] = df_enriched['serper_jsons_lists'].apply(parse_serper_results)
    def call_openai_api_2(row):
        Article_Content = row['Article_Content']
        context_data = row['bing_final']
        current_date = row['date_ist']


        prompt = f"""Objective: Extract and carry out advanced analysis on news event to determine
        its impact on various business sectors, emphasizing actionable insights for a broad
        audience of corporate executives, including Chief Security Officers, Risk Officers, and
        other CXOs. Use a standardized scoring system for clarity and ensure the analysis is
        universally applicable, regardless of the reader's specific industry or location.
        Process news articles critically, identifying and mitigating any inherent biases.
        Focus strictly on factual content, and maintain neutrality regardless of the content's
        biases. Your primary objective is to be objective and impartial based on the data provided, and generate a risk alert using the news Event(Article_Content).

        Instructions:

        When generating the risk alert, use the provided `context_data` to enhance your understanding and analysis of the primary event described in the `Article_Content`. This additional `context_data` may include related events, historical precedents, industry trends, or any other relevant information that can deepen your insight into the situation.

        Your primary focus should remain on the `Article_Content`, ensuring that the alert is directly centered on this event. However, the `context_data` should be leveraged to provide a more comprehensive and nuanced analysis, allowing you to:

        1. Contextualize the Event: Place the event within a broader context, considering how related incidents or patterns might affect its significance or impact.

        2. Enhance Insights: Use the `context_data` to offer more detailed and advanced insights, especially in areas where the original article may lack depth.

        3. Support Scoring and Categorization: Draw upon the `context_data` to more accurately assign impact, urgency, severity, and likelihood scores, as well as to determine the appropriate risk category and alert level.

        4. Maintain Objectivity and Neutrality: Continue to process the `Article_Content` critically, focusing on factual content and mitigating biases, while using the `context_data` to support an objective and well-rounded analysis.

        The structure, format, and overall objective of the alert should remain unchanged, with the additional `context_data` serving solely to enhance the quality and depth of the insights provided.

        1. Title and Main Event Analysis:
        - Task: Generate a concise title that captures the essence of the news event, including its main
        issue and location.
        - Summarize the primary subject of the news event, highlighting both direct and indirect
        implications for businesses universally. While highlighting implications, be pragmatic,
        do not create a false sense of emergency. If an event does not require any action to be
        taken, advice what should suffice for businesses operating in the region.
        - Output: 'Title': "<Concise Title>", 'Main Topic/Event': "<Summary>",

        2. Extraction of Key Details and Contextual Elements:
        - Incident Details:
            - Event date and time
            - Description of the incident
            - Location
        - Causes and Contributing Factors:
            - Likely causes of the incident
            - Detailed technical explanations related to the causes (e.g., how electrical short circuits cause fires, impact of gas cylinder explosions)
        - Safety Violations and Lapses (if applicable):
            - Specific safety violations or lapses
            - Relevant laws, regulations, and standards that may have been violated
            - Historical context and comparison with similar incidents
            - Potential legal and regulatory implications
        - Impact Details:
            - Number of fatalities, injuries, materlial losses, financial loss etc
            - Affected demographics (e.g., nationality, age, etc.)
            - Broader impact on businesses and sectors
        - Response and Mitigation:
            - Actions taken by authorities and organizations
            - Ongoing response and investigations
            - Long-term strategies and future implications
        - Additional Context:
            - Any other relevant information or background

        3. Sector and Industry Analysis (GICS Standard):
        - Direct and Indirect Impact Analysis:
            - Identify direct and indirect effects on GICS categories (Sector, Industry Group, Industry, Sub-Industry). Use advanced insights to discuss potential impacts.
            - Provide a universal perspective on potential impacts. Include innovative solutions and advanced strategies for risk management.

        - Non-GICS Impact Analysis:
            - Dynamically identify and categorize impacts not covered by GICS, such as 'Government', 'Public', or other relevant categories, with a focus on immediate impact on businesses operating in the region. Discuss regulatory and compliance implications and provide advanced recommendations for mitigation.
            - Output: 'GICS Impact': {{'Direct': {{<details>}}, 'Indirect': {{<details>}}, 'Non-GICS Impact': "<Category and Rationale>

        4. Scoring System:
        - Assign numeric scores to evaluate the event's impact, urgency, intensity, and likelihood, ensuring the system is intuitive and universally applicable.
            - 'Overall Impact Score': From -1 (negative) to +1 (positive). Measures the potential repercussions of an event or situation on business operations or market conditions. A higher score indicates more significant consequences that could disrupt business continuity or cause substantial economic loss.
            - 'Overall Immediacy Score': From 0 (no urgency) to 10 (immediate action needed). Reflects the urgency of addressing a risk. A higher score signifies that the risk demands prompt attention and response to prevent escalation or to capitalize on emerging opportunities quickly.
            - 'Overall Severity Score': From 0 (minimal severity) to 10 (maximum severity). Assesses the extent of harm or damage that could result from the risk if not adequately managed. Higher scores denote situations that could lead to severe outcomes, affecting safety, regulatory compliance, or environmental stability.
            - 'Likelihood Score': From 0 (unlikely) to 10 (very likely). Evaluates the probability of a risk occurring. This score helps prioritize risks, with higher values indicating a greater chance of the risk manifesting, thereby necessitating more focused risk management strategies.
        - Output: Each score provided with a brief explanation for universal understanding.

        5. Comprehensive Analysis:
        - Synthesize the event's categorized impacts and scores into a detailed yet universally applicable analysis.
        - Provide separate analysis for directly and indirectly impacted industries and another section titled “Advice to businesses operating in the region and advice to travellers in the region,” explaining why it impacts them. Focus on advanced strategies for risk mitigation, operational resilience, and strategic foresight. Use advanced knowledge from your own models knowledgebase, apply and advice innovative approaches.
        - Offer actionable insights tailored for a broad executive audience, highlighting both immediate measures and longer-term strategic considerations. Do not mention GICS or NON-GICS in any generated content
        - Output: 'Analysis':  "Direct impact: <advanced actionable insights>, Indirect impact: <advanced actionable insights>"
        
        6. Topic Categorisation:
        - Categorize the event into a relevant risk category.
        - Output: 'Category': "<Risk category>"

        7. Alert level identification:
        - Classify the event into one of the three alert levels based on the criteria:
            - Red Alert (Critical):
                - Criteria:
                    - Immediacy Score: 8-10 (immediate action required from businesses and corporates)
                    - Severity Score: 7-10 (potential for significant harm or damage to businesses and corporates)
                    - Likelihood Score: 7-10 (high probability of the event occurring)
                    - Impact Score: -0.5 to -1 (substantial negative impact on business operations)
                - Business Impact Description: "Poses an immediate and severe threat to safety and continuity, demanding urgent intervention to mitigate potential large-scale disruptions or losses."
                - Management Actions: "Quick decision-making, activation of emergency plans, and possibly external support are necessary."
            - Yellow Alert (Moderate):
                - Criteria:
                    - Immediacy Score: 4-7 (prompt attention needed but not immediate action from businesses and corporates)
                    - Severity Score: 4-6 (could escalate if not managed)
                    - Likelihood Score: 4-6 (moderately likely to occur)
                    - Impact Score: -0.2 to -0.5 (noticeable negative impacts on business operations)
                - Business Impact Description: "Represents a developing threat that could disrupt operations or result in financial impact, requiring increased preparedness and monitoring."
                - Management Actions: "Regular reviews, enhanced monitoring, and preparations for potential escalation are recommended."
            - Green Alert (Low):
                - Criteria:
                    - Immediacy Score: 0-3 (minimal urgency to businesses and corporates)
                    - Severity Score: 0-3 (minimal to no severity to businesses and corporates)
                    - Likelihood Score: 0-3 (unlikely to occur)
                    - Impact Score: -0.1 to 0.1 (negligible impact on business operations)
                - Business Impact Description: "Involves routine developments or updates with minimal immediate implications, generally managed through standard operational protocols."
                - Management Actions: "Regular monitoring and maintenance activities are usually sufficient."
        - Output: 'Alert Level': "<Alert level>"

        8. Location Identification:
        - Identify all impacted locations with their corresponding country in the article.
        - Output: 'Location': "<Identified Locations>"
        
        Output Format:
        Ensure the analysis is structured for easy interpretation, using a nested dictionary format to separate and clarify direct, indirect, and non-GICS impacts, alongside a coherent scoring rationale.
        {{
        "Title": "<Concise Title summarizing the event>",
        "Main Topic/Event": "<Event Summary>",
        "Extraction of Key Details and Contextual Elements": {{
            "Incident Details": {{
            "Event date and time": "<Event date and time>",
            "Description of the incident": "<Description of the incident>",
            "Location": "<Location>"
            }},
            "Causes and Contributing Factors": {{
            "Likely causes of the incident": "<Likely causes of the incident>",
            "Technical details related to the causes": "<Technical details related to the causes>"
            }},
            "Safety Violations and Lapses": {{
            "Specific safety violations or lapses": "<Specific safety violations or lapses>",
            "Relevant laws, regulations, and standards that may have been violated": "<Relevant laws, regulations, and standards that may have been violated>",
            "Potential legal and regulatory implications": "<Potential legal and regulatory implications>"
            }},
            "Impact Details": {{
            "Number of fatalities and injuries": "<Number of fatalities and injuries>",
            "Affected demographics": "<Affected demographics>",
            "Impact on businesses and sectors": "<Impact on businesses and sectors>"
            }},
            "Response and Mitigation": {{
            "Actions taken by authorities and organizations": "<Actions taken by authorities and organizations>",
            "Ongoing response and investigations": "<Ongoing response and investigations>"
            }},
            "Additional Context": {{
            "Any other relevant information or background": "<Any other relevant information or background>"
            }}
        }},
        "GICS Impact": {{
            "Direct": {{"Sector": "<sector>", "Industry Group": "<industry group>", "Industry": "<industry>", "Sub-Industry": "<sub-industry>"}},
            "Indirect": {{"Sector": "<sector>", "Industry Group": "<industry group>", "Industry": "<industry>", "Sub-Industry": "<sub-industry>"}}
        }},
        "Non-GICS Impact": "Government/Public/Other: <impact description>",
        "Scores": {{
            "Overall Impact Score": <value>,
            "Overall Immediacy Score": <value>,
            "Overall Severity Score": <value>,
            "Likelihood Score": <value>
        }},
        "Analysis": "Direct impact: <advanced actionable insights>, Indirect impact: <advanced actionable insights>",
        "Category": "<Assigned risk category>",
        "Alert Level": "<Assigned alert level>",
        "Location": "<Identified Locations>",
        "Detailed Description": "<Comprehensive and insightful description with advanced insights>"
        }}

        Event(Article_Content): {Article_Content},
        context_data: {context_data},
        current_datetime: {current_date}
            """

    # Call OpenAI's API to generate the response
        try:
            client = OpenAI(
            api_key=os.environ["OPENAI_API_KEY"],
        )

            chat_completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                {"role": "system", "content": "You are an expert at extracting detailed information and providing highly advanced analysis. You can tap into your own vast resources of world knowledgebase from your training data, effectivley and wisely apply that knowledgebase to analyse each data point in extreme depth and apply this to the provided news article to generate an extremely technical, tactical, pragmatic, comprehensive and insightful in depth and detailed analysis."},
                {"role": "user", "content": prompt}
            ],response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": "beep_alert",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "Title": {
                        "type": "string",
                        "description": "Concise title summarizing the event."
                    },
                    "Main Topic/Event": {
                        "type": "string",
                        "description": "Summary of the main topic or event."
                    },
                    "Extraction of Key Details and Contextual Elements": {
                        "type": "object",
                        "properties": {
                            "Incident Details": {
                                "type": "object",
                                "properties": {
                                    "Event date and time": {
                                        "type": "string",
                                        "description": "Date and time of the event."
                                    },
                                    "Description of the incident": {
                                        "type": "string",
                                        "description": "Detailed description of the incident."
                                    },
                                    "Location": {
                                        "type": "string",
                                        "description": "Location where the incident occurred."
                                    }
                                },
                                "required": [
                                    "Event date and time",
                                    "Description of the incident",
                                    "Location"
                                ],
                                "additionalProperties": False
                            },
                            "Causes and Contributing Factors": {
                                "type": "object",
                                "properties": {
                                    "Likely causes of the incident": {
                                        "type": "string",
                                        "description": "Probable causes of the incident."
                                    },
                                    "Technical details related to the causes": {
                                        "type": "string",
                                        "description": "Technical explanations of the causes."
                                    }
                                },
                                "required": [
                                    "Likely causes of the incident",
                                    "Technical details related to the causes"
                                ],
                                "additionalProperties": False
                            },
                            "Safety Violations and Lapses": {
                                "type": "object",
                                "properties": {
                                    "Specific safety violations or lapses": {
                                        "type": "string",
                                        "description": "Details of any safety violations."
                                    },
                                    "Relevant laws, regulations, and standards that may have been violated": {
                                        "type": "string",
                                        "description": "Laws or regulations potentially violated."
                                    },
                                    "Potential legal and regulatory implications": {
                                        "type": "string",
                                        "description": "Possible legal consequences."
                                    }
                                },
                                "required": [
                                    "Specific safety violations or lapses",
                                    "Relevant laws, regulations, and standards that may have been violated",
                                    "Potential legal and regulatory implications"
                                ],
                                "additionalProperties": False
                            },
                            "Impact Details": {
                                "type": "object",
                                "properties": {
                                    "Number of fatalities and injuries": {
                                        "type": "string",
                                        "description": "Count of fatalities and injuries."
                                    },
                                    "Affected demographics": {
                                        "type": "string",
                                        "description": "Demographics of those affected."
                                    },
                                    "Impact on businesses and sectors": {
                                        "type": "string",
                                        "description": "How businesses and sectors are impacted."
                                    }
                                },
                                "required": [
                                    "Number of fatalities and injuries",
                                    "Affected demographics",
                                    "Impact on businesses and sectors"
                                ],
                                "additionalProperties": False
                            },
                            "Response and Mitigation": {
                                "type": "object",
                                "properties": {
                                    "Actions taken by authorities and organizations": {
                                        "type": "string",
                                        "description": "Response measures taken."
                                    },
                                    "Ongoing response and investigations": {
                                        "type": "string",
                                        "description": "Current status of responses and investigations."
                                    }
                                },
                                "required": [
                                    "Actions taken by authorities and organizations",
                                    "Ongoing response and investigations"
                                ],
                                "additionalProperties": False
                            },
                            "Additional Context": {
                                "type": "object",
                                "properties": {
                                    "Any other relevant information or background": {
                                        "type": "string",
                                        "description": "Additional pertinent information."
                                    }
                                },
                                "required": [
                                    "Any other relevant information or background"
                                ],
                                "additionalProperties": False
                            }
                        },
                        "required": [
                            "Incident Details",
                            "Causes and Contributing Factors",
                            "Safety Violations and Lapses",
                            "Impact Details",
                            "Response and Mitigation",
                            "Additional Context"
                        ],
                        "additionalProperties": False
                    },
                    "GICS Impact": {
                        "type": "object",
                        "properties": {
                            "Direct": {
                                "type": "object",
                                "properties": {
                                    "Sector": {
                                        "type": "string",
                                        "description": "Directly impacted sector."
                                    },
                                    "Industry Group": {
                                        "type": "string",
                                        "description": "Directly impacted industry group."
                                    },
                                    "Industry": {
                                        "type": "string",
                                        "description": "Directly impacted industry."
                                    },
                                    "Sub-Industry": {
                                        "type": "string",
                                        "description": "Directly impacted sub-industry."
                                    }
                                },
                                "required": [
                                    "Sector",
                                    "Industry Group",
                                    "Industry",
                                    "Sub-Industry"
                                ],
                                "additionalProperties": False
                            },
                            "Indirect": {
                                "type": "object",
                                "properties": {
                                    "Sector": {
                                        "type": "string",
                                        "description": "Indirectly impacted sector."
                                    },
                                    "Industry Group": {
                                        "type": "string",
                                        "description": "Indirectly impacted industry group."
                                    },
                                    "Industry": {
                                        "type": "string",
                                        "description": "Indirectly impacted industry."
                                    },
                                    "Sub-Industry": {
                                        "type": "string",
                                        "description": "Indirectly impacted sub-industry."
                                    }
                                },
                                "required": [
                                    "Sector",
                                    "Industry Group",
                                    "Industry",
                                    "Sub-Industry"
                                ],
                                "additionalProperties": False
                            }
                        },
                        "required": [
                            "Direct",
                            "Indirect"
                        ],
                        "additionalProperties": False
                    },
                    "Non-GICS Impact": {
                        "type": "string",
                        "description": "Impact on government, public, or other non-GICS categories."
                    },
                    "Scores": {
                        "type": "object",
                        "properties": {
                            "Overall Impact Score": {
                                "type": "number",
                                "description": "From -1 (negative) to +1 (positive)."
                            },
                            "Overall Immediacy Score": {
                                "type": "number",
                                "description": "From 0 (no urgency) to 10 (immediate action needed)."
                            },
                            "Overall Severity Score": {
                                "type": "number",
                                "description": "From 0 (minimal severity) to 10 (maximum severity)."
                            },
                            "Likelihood Score": {
                                "type": "number",
                                "description": "From 0 (unlikely) to 10 (very likely)."
                            }
                        },
                        "required": [
                            "Overall Impact Score",
                            "Overall Immediacy Score",
                            "Overall Severity Score",
                            "Likelihood Score"
                        ],
                        "additionalProperties": False
                    },
                    "Analysis": {
                        "type": "string",
                        "description": "Advanced actionable insights for direct and indirect impacts."
                    },
                    "Category": {
                        "type": "string",
                        "description": "Assigned risk category."
                    },
                    "Alert Level": {
                        "type": "string",
                        "description": "Assigned alert level (Red, Yellow, Green)."
                    },
                    "Location": {
                        "type": "string",
                        "description": "Identified impacted locations."
                    },
                    "Detailed Description": {
                        "type": "string",
                        "description": "Comprehensive and insightful description with advanced insights."
                    }
                },
                "required": [
                    "Title",
                    "Main Topic/Event",
                    "Extraction of Key Details and Contextual Elements",
                    "GICS Impact",
                    "Non-GICS Impact",
                    "Scores",
                    "Analysis",
                    "Category",
                    "Alert Level",
                    "Location",
                    "Detailed Description"
                ],
                "additionalProperties": False
            }
        }
    }

            
        )

        # Extract output from OpenAI's response
            gpt_output=chat_completion.choices[0].message.content
            return gpt_output
        except Exception as e:
            return str(e)

    df_enriched['details'] = df_enriched.apply(call_openai_api_2, axis=1)
    def parse_beep(beep_json):
        """
        Parse a JSON string to extract and flatten the relevant fields into a dictionary.
    
        Parameters:
        beep_json (str): The JSON string to parse.
    
        Returns:
        dict: A dictionary with the flattened structure.
        """
        try:
            data = json.loads(beep_json)
        except json.JSONDecodeError as e:
            print(f"JSONDecodeError: {e} - Failed to parse JSON: {beep_json}")
            return None  # Return None to indicate failure
    
        # Extract and flatten the relevant fields with failsafe
        flat_data = {
            "Title": data.get("Title", ""),
            "Main_Topic_Event": data.get("Main Topic/Event", ""),
            "Incident_Event_date_and_time": data.get("Extraction of Key Details and Contextual Elements", {}).get("Incident Details", {}).get("Event date and time", ""),
            "Incident_Description_of_the_incident": data.get("Extraction of Key Details and Contextual Elements", {}).get("Incident Details", {}).get("Description of the incident", ""),
            "Incident_Location": data.get("Extraction of Key Details and Contextual Elements", {}).get("Incident Details", {}).get("Location", ""),
            "Causes_Likely_causes_of_the_incident": data.get("Extraction of Key Details and Contextual Elements", {}).get("Causes and Contributing Factors", {}).get("Likely causes of the incident", ""),
            "Causes_Technical_details_related_to_the_causes": data.get("Extraction of Key Details and Contextual Elements", {}).get("Causes and Contributing Factors", {}).get("Technical details related to the causes", ""),
            "Safety_Violations_or_Lapses": data.get("Extraction of Key Details and Contextual Elements", {}).get("Safety Violations and Lapses", {}).get("Specific safety violations or lapses", ""),
            "Safety_Relevant_laws": data.get("Extraction of Key Details and Contextual Elements", {}).get("Safety Violations and Lapses", {}).get("Relevant laws, regulations, and standards that may have been violated", ""),
            "Safety_Potential_legal_implications": data.get("Extraction of Key Details and Contextual Elements", {}).get("Safety Violations and Lapses", {}).get("Potential legal and regulatory implications", ""),
            "Impact_Number_of_fatalities_and_injuries": data.get("Extraction of Key Details and Contextual Elements", {}).get("Impact Details", {}).get("Number of fatalities and injuries", ""),
            "Impact_Affected_demographics": data.get("Extraction of Key Details and Contextual Elements", {}).get("Impact Details", {}).get("Affected demographics", ""),
            "Impact_on_businesses_and_sectors": data.get("Extraction of Key Details and Contextual Elements", {}).get("Impact Details", {}).get("Impact on businesses and sectors", ""),
            "Response_Actions_taken": data.get("Extraction of Key Details and Contextual Elements", {}).get("Response and Mitigation", {}).get("Actions taken by authorities and organizations", ""),
            "Response_Ongoing_investigations": data.get("Extraction of Key Details and Contextual Elements", {}).get("Response and Mitigation", {}).get("Ongoing response and investigations", ""),
            "Additional_Context": data.get("Extraction of Key Details and Contextual Elements", {}).get("Additional Context", {}).get("Any other relevant information or background", ""),
            "GICS_Direct_Sector": data.get("GICS Impact", {}).get("Direct", {}).get("Sector", ""),
            "GICS_Direct_Industry_Group": data.get("GICS Impact", {}).get("Direct", {}).get("Industry Group", ""),
            "GICS_Direct_Industry": data.get("GICS Impact", {}).get("Direct", {}).get("Industry", ""),
            "GICS_Direct_Sub_Industry": data.get("GICS Impact", {}).get("Direct", {}).get("Sub-Industry", ""),
            "GICS_Indirect_Sector": data.get("GICS Impact", {}).get("Indirect", {}).get("Sector", ""),
            "GICS_Indirect_Industry_Group": data.get("GICS Impact", {}).get("Indirect", {}).get("Industry Group", ""),
            "GICS_Indirect_Industry": data.get("GICS Impact", {}).get("Indirect", {}).get("Industry", ""),
            "GICS_Indirect_Sub_Industry": data.get("GICS Impact", {}).get("Indirect", {}).get("Sub-Industry", ""),
            "Non_GICS_Impact": data.get("Non-GICS Impact", ""),
            "Scores_Overall_Impact_Score": data.get("Scores", {}).get("Overall Impact Score", ""),
            "Scores_Overall_Immediacy_Score": data.get("Scores", {}).get("Overall Immediacy Score", ""),
            "Scores_Overall_Severity_Score": data.get("Scores", {}).get("Overall Severity Score", ""),
            "Scores_Likelihood_Score": data.get("Scores", {}).get("Likelihood Score", ""),
            "Analysis": data.get("Analysis", ""),
            "Category": data.get("Category", ""),
            "Alert_Level": data.get("Alert Level", ""),
            "Location": data.get("Location", ""),
            "Detailed_Description": data.get("Detailed Description", "")
        }
    
        return flat_data

    def apply_parse_beep(df):
        """
        Apply the parse_beep function to each row of the DataFrame on the 'details' column,
        retrying parsing if initial parsing fails. Applies additional retry if the second attempt fails.
        Drops rows if parsing fails after all retries.
        
        Parameters:
        df (pd.DataFrame): The DataFrame containing a column 'details' with JSON strings.
        
        Returns:
        pd.DataFrame: The original DataFrame with new parsed columns added and rows dropped where parsing failed.
        """
        def parse_row(row):
            parsed_result = parse_beep(row['details'])
            if parsed_result is None:  # Check if parsing failed the first time
                try:
                    # First retry: Regenerate 'details' using the call_openai_api function and attempt to parse again
                    new_details = call_openai_api_2(row)
                    parsed_result = parse_beep(new_details)
                    if parsed_result is None:
                        # Second retry: Try regenerating 'details' once more if the first retry failed
                        new_details = call_openai_api_2(row)
                        parsed_result = parse_beep(new_details)
                        if parsed_result is None:
                            return None  # If parsing still fails, return None to signal dropping the row
                        else:
                            row['details'] = new_details  # Update 'details' with new output
                    else:
                        row['details'] = new_details  # Update 'details' with new output
                except Exception as e:
                    print(f"Exception during regeneration at row {row.name}: {e}")
                    return None
            return parsed_result

        # Apply the parse_row function to each row and collect parsed data
        parsed_data = df.apply(parse_row, axis=1)
        parsed_data = parsed_data.dropna()  # Drop rows where parsing returned None

        # Flatten and normalize the parsed data, then merge it back to the original DataFrame
        if not parsed_data.empty:
            parsed_df = pd.DataFrame(parsed_data.tolist())  # Convert list of dicts to DataFrame
            df = df.loc[parsed_data.index].reset_index(drop=True)  # Keep only rows that were successfully parsed
            df = pd.concat([df.reset_index(drop=True), parsed_df.reset_index(drop=True)], axis=1)
            df.dropna(subset=['Title'], inplace=True)  # Assuming 'Title' is a mandatory field

        return df
    df2 = apply_parse_beep(df_enriched)
    # Define the regular expressions to extract the direct and indirect impacts
    direct_impact_pattern = r"Direct impact: ([^\.]+)\."
    indirect_impact_pattern = r"Indirect impact: ([^\.]+)\."

    # Function to extract direct and indirect impacts, handling both string and dictionary formats
    def extract_impact(text, pattern=None, key=None):
        if isinstance(text, str):  # If it's a string, use regex to extract the impact
            match = re.search(pattern, text)
            return match.group(1) if match else None
        elif isinstance(text, dict):  # If it's a dictionary, extract based on the key
            return text.get(key, None)
        return None

    # Assuming merged_df already exists and has an 'Analysis' column

    # Extract the Direct Impact into a new column (handling both formats)
    df2['Analysis_DirectImpact'] = df2['Analysis'].apply(
        lambda x: extract_impact(x, pattern=direct_impact_pattern, key='Direct impact')
    )

    # Extract the Indirect Impact into a new column (handling both formats)
    df2['Analysis_IndirectImpact'] = df2['Analysis'].apply(
        lambda x: extract_impact(x, pattern=indirect_impact_pattern, key='Indirect impact')
    )
    df2['effectiveDate'] = pd.Timestamp.now()
    # Standardize the Alert_Level column to be case-insensitive
    df2['Alert_Level'] = np.where(df2['Alert_Level'].str.lower().str.contains('red'), 'Red Alert',
                            np.where(df2['Alert_Level'].str.lower().str.contains('yellow'), 'Yellow Alert',
                            np.where(df2['Alert_Level'].str.lower().str.contains('green'), 'Green Alert',
                                    df2['Alert_Level'])))
                                        
    return df2