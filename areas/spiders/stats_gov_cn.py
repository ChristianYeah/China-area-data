import scrapy


class StatsGovCnSpider(scrapy.Spider):
    name = 'stats-gov-cn'
    allowed_domains = ['www.stats.gov.cn']
    start_urls = [
        'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/index.html'
    ]
    next_area_type = {'province': 'city', 'city': 'county', 'county': 'town', 'town': 'village'}
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8',
        'RETRY_TIMES': 9999999999,
        'LOG_LEVEL': 'INFO',
        'CONCURRENT_REQUESTS': 128
    }

    def start_request(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url, callback=self.parse, dont_filter=True
            )

    def parse(self, response):
        area_type = response.meta.get("next_area_type", 'province')
        for code, name, link in self.__parser(area_type, response):
            data = response.meta.get('data', {}).copy()
            if area_type != 'village':
                data.update({"%s_code" % area_type: code, "%s_name" % area_type: name})
                yield scrapy.Request(link, callback=self.parse, dont_filter=True, meta={
                    "next_area_type": self.next_area_type.get(area_type, None), "data": data
                })
            else:
                data.update({"%s_code" % area_type: code, "%s_name" % area_type: link, "area_type_code": name})
                yield data

    @staticmethod
    def __parser(area_type, response):
        xpath = "//table[@class='%stable']/tr[@class='%str']" % (area_type, area_type)
        if area_type == 'province':
            return [(
                a.xpath("@href").get().replace(".html", ""),
                a.xpath("text()").get(),
                response.urljoin(a.xpath("@href").get())
            ) for a in response.xpath(xpath + "/td/a")]
        elif area_type == 'village':
            return [(tr.xpath("td[1]/text()").get(), tr.xpath("td[2]/text()").get(), tr.xpath("td[3]/text()").get(),
                     ) for tr in response.xpath(xpath)]
        else:
            return [(tr.xpath("td[1]/a/text()").get(), tr.xpath("td[2]/a/text()").get(),
                     response.urljoin(tr.xpath("td[2]/a/@href").get()),) for tr in response.xpath(xpath)]
