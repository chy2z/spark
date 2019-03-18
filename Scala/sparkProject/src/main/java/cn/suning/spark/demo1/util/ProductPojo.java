package cn.suning.spark.demo1.util;

import java.io.Serializable;

public class ProductPojo
        implements Serializable
{
    private static final long serialVersionUID = 7841936356959542824L;
    private String categoryId;
    private String partnumber;
    private String catentry_Id;
    private String catalog;
    private String catentdesc;
    private String title;
    private String brand_Id;
    private String brand_Name;
    private String brand_Name_FacetAll;
    private String brand_Id_Name;
    private String virtual_Name;
    private String description;
    private String auxdescription;
    private String availabilitydate;
    private String totalCount;
    private String realTotalCount;
    private String totalCountScore;
    private String inventoryScore;
    private String inventory;
    private String swl_Inventory;
    private String all_Inventory;
    private String two_groupName;
    private String three_groupName;
    private String groupIDCopy;
    private String three_groupExtName;
    private String groupIDCombination;
    private String catgroup_all_cache;
    private String articlePoint;
    private String countOfarticle;
    private String praiseRate;
    private String listPraiseRate;
    private String listCommentScore;
    private String listBrandScore;
    private String _DY_price;
    private String _DY_swl_price;
    private String _DY_best_price;
    private String supplier_num;
    private String pricing;
    private String discount;
    private String cityIds;
    private String swl_cityIds;
    private String all_cityIds;
    private String author;
    private String printingDate;
    private String media;
    private String isbn;
    private String btcode;
    private String btsort;
    private String btgroup;
    private String unit_searchable_attr;
    private String _DYM_attrId;
    private String _DYM_attr_unite;
    private String goodsType;
    private String goodsOrder;
    private String maternalOrder;
    private String cosmeticsOrder;
    private String importBookOrder;
    private String magazineOrder;
    private String cdOrder;
    private String mdmGroupId;
    private String contractInfos;
    private String promotions;
    private String activity;
    private String editGroupIDCopy;
    private String editGroupIDCombination;
    private String isNew;
    private String find_source;
    private String is_vaild_citys;
    private String _DY_activityBeginTime;
    private String _DY_activityEndTime;
    private String _DC_SOLR_SHARD;
    private String _DC_PRODUCT_TYPE;
    private String _DC_DEL_FLAG;
    private String supre_sale_type;
    private String channel;
    private String comprehensiveSortScores;
    private String clickCount;
    private String saleCount;
    private String salesAmount;
    private String commodityFlag;
    private String mainCommodityId;
    private String brand_homeStream;
    private String goods_gender;
    private String _DC_UPDATE_TIME;
    private String extField_1;
    private String extField_2;

    public ProductPojo()
    {
    }

    public ProductPojo(String partnumber, String catentry_Id, String catalog, String catentdesc, String title, String brand_Id, String brand_Name, String brand_Name_FacetAll, String brand_Id_Name, String virtual_Name, String description, String auxdescription, String availabilitydate, String totalCount, String realTotalCount, String totalCountScore, String inventoryScore, String inventory, String swl_Inventory, String all_Inventory, String two_groupName, String three_groupName, String groupIDCopy, String three_groupExtName, String groupIDCombination, String catgroup_all_cache, String articlePoint, String countOfarticle, String praiseRate, String listPraiseRate, String listCommentScore, String listBrandScore, String _DY_price, String _DY_swl_price, String _DY_best_price, String supplier_num, String pricing, String discount, String cityIds, String swl_cityIds, String all_cityIds, String author, String printingDate, String media, String isbn, String btcode, String btsort, String btgroup, String unit_searchable_attr, String _DYM_attrId, String _DYM_attr_unite, String goodsType, String goodsOrder, String maternalOrder, String cosmeticsOrder, String importBookOrder, String magazineOrder, String cdOrder, String mdmGroupId, String contractInfos, String promotions, String activity, String editGroupIDCopy, String editGroupIDCombination, String isNew, String find_source, String is_vaild_citys, String _DY_activityBeginTime, String _DY_activityEndTime, String _DC_SOLR_SHARD, String _DC_PRODUCT_TYPE, String _DC_DEL_FLAG, String supre_sale_type, String channel, String comprehensiveSortScores, String clickCount, String saleCount, String salesAmount, String commodityFlag, String mainCommodityId, String brand_homeStream, String goods_gender, String _DC_UPDATE_TIME)
    {
        this.partnumber = partnumber;
        this.catentry_Id = catentry_Id;
        this.catalog = catalog;
        this.catentdesc = catentdesc;
        this.title = title;
        this.brand_Id = brand_Id;
        this.brand_Name = brand_Name;
        this.brand_Name_FacetAll = brand_Name_FacetAll;
        this.brand_Id_Name = brand_Id_Name;
        this.virtual_Name = virtual_Name;
        this.description = description;
        this.auxdescription = auxdescription;
        this.availabilitydate = availabilitydate;
        this.totalCount = totalCount;
        this.realTotalCount = realTotalCount;
        this.totalCountScore = totalCountScore;
        this.inventoryScore = inventoryScore;
        this.inventory = inventory;
        this.swl_Inventory = swl_Inventory;
        this.all_Inventory = all_Inventory;
        this.two_groupName = two_groupName;
        this.three_groupName = three_groupName;
        this.groupIDCopy = groupIDCopy;
        this.three_groupExtName = three_groupExtName;
        this.groupIDCombination = groupIDCombination;
        this.catgroup_all_cache = catgroup_all_cache;
        this.articlePoint = articlePoint;
        this.countOfarticle = countOfarticle;
        this.praiseRate = praiseRate;
        this.listPraiseRate = listPraiseRate;
        this.listCommentScore = listCommentScore;
        this.listBrandScore = listBrandScore;
        this._DY_price = _DY_price;
        this._DY_swl_price = _DY_swl_price;
        this._DY_best_price = _DY_best_price;
        this.supplier_num = supplier_num;
        this.pricing = pricing;
        this.discount = discount;
        this.cityIds = cityIds;
        this.swl_cityIds = swl_cityIds;
        this.all_cityIds = all_cityIds;
        this.author = author;
        this.printingDate = printingDate;
        this.media = media;
        this.isbn = isbn;
        this.btcode = btcode;
        this.btsort = btsort;
        this.btgroup = btgroup;
        this.unit_searchable_attr = unit_searchable_attr;
        this._DYM_attrId = _DYM_attrId;
        this._DYM_attr_unite = _DYM_attr_unite;
        this.goodsType = goodsType;
        this.goodsOrder = goodsOrder;
        this.maternalOrder = maternalOrder;
        this.cosmeticsOrder = cosmeticsOrder;
        this.importBookOrder = importBookOrder;
        this.magazineOrder = magazineOrder;
        this.cdOrder = cdOrder;
        this.mdmGroupId = mdmGroupId;
        this.contractInfos = contractInfos;
        this.promotions = promotions;
        this.activity = activity;
        this.editGroupIDCopy = editGroupIDCopy;
        this.editGroupIDCombination = editGroupIDCombination;
        this.isNew = isNew;
        this.find_source = find_source;
        this.is_vaild_citys = is_vaild_citys;
        this._DY_activityBeginTime = _DY_activityBeginTime;
        this._DY_activityEndTime = _DY_activityEndTime;
        this._DC_SOLR_SHARD = _DC_SOLR_SHARD;
        this._DC_PRODUCT_TYPE = _DC_PRODUCT_TYPE;
        this._DC_DEL_FLAG = _DC_DEL_FLAG;
        this.supre_sale_type = supre_sale_type;
        this.channel = channel;
        this.comprehensiveSortScores = comprehensiveSortScores;
        this.clickCount = clickCount;
        this.saleCount = saleCount;
        this.salesAmount = salesAmount;
        this.commodityFlag = commodityFlag;
        this.mainCommodityId = mainCommodityId;
        this.brand_homeStream = brand_homeStream;
        this.goods_gender = goods_gender;
        this._DC_UPDATE_TIME = _DC_UPDATE_TIME;
    }

    public String getCategoryId()
    {
        return this.categoryId;
    }
    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }
    public String getExtField_1() {
        return this.extField_1;
    }

    public void setExtField_1(String extField_1) {
        this.extField_1 = extField_1;
    }

    public String getExtField_2() {
        return this.extField_2;
    }

    public void setExtField_2(String extField_2) {
        this.extField_2 = extField_2;
    }

    public String getPartnumber() {
        return this.partnumber;
    }

    public void setPartnumber(String partnumber) {
        this.partnumber = partnumber;
    }

    public String getCatentry_Id() {
        return this.catentry_Id;
    }

    public void setCatentry_Id(String catentry_Id) {
        this.catentry_Id = catentry_Id;
    }

    public String getCatalog() {
        return this.catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getCatentdesc() {
        return this.catentdesc;
    }

    public void setCatentdesc(String catentdesc) {
        this.catentdesc = catentdesc;
    }

    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getBrand_Id() {
        return this.brand_Id;
    }

    public void setBrand_Id(String brand_Id) {
        this.brand_Id = brand_Id;
    }

    public String getBrand_Name() {
        return this.brand_Name;
    }

    public void setBrand_Name(String brand_Name) {
        this.brand_Name = brand_Name;
    }

    public String getBrand_Name_FacetAll() {
        return this.brand_Name_FacetAll;
    }

    public void setBrand_Name_FacetAll(String brand_Name_FacetAll) {
        this.brand_Name_FacetAll = brand_Name_FacetAll;
    }

    public String getBrand_Id_Name() {
        return this.brand_Id_Name;
    }

    public void setBrand_Id_Name(String brand_Id_Name) {
        this.brand_Id_Name = brand_Id_Name;
    }

    public String getVirtual_Name() {
        return this.virtual_Name;
    }

    public void setVirtual_Name(String virtual_Name) {
        this.virtual_Name = virtual_Name;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getAuxdescription() {
        return this.auxdescription;
    }

    public void setAuxdescription(String auxdescription) {
        this.auxdescription = auxdescription;
    }

    public String getAvailabilitydate() {
        return this.availabilitydate;
    }

    public void setAvailabilitydate(String availabilitydate) {
        this.availabilitydate = availabilitydate;
    }

    public String getTotalCount() {
        return this.totalCount;
    }

    public void setTotalCount(String totalCount) {
        this.totalCount = totalCount;
    }

    public String getRealTotalCount() {
        return this.realTotalCount;
    }

    public void setRealTotalCount(String realTotalCount) {
        this.realTotalCount = realTotalCount;
    }

    public String getTotalCountScore() {
        return this.totalCountScore;
    }

    public void setTotalCountScore(String totalCountScore) {
        this.totalCountScore = totalCountScore;
    }

    public String getInventoryScore() {
        return this.inventoryScore;
    }

    public void setInventoryScore(String inventoryScore) {
        this.inventoryScore = inventoryScore;
    }

    public String getInventory()
    {
        return this.inventory;
    }

    public void setInventory(String inventory) {
        this.inventory = inventory;
    }

    public String getSwl_Inventory() {
        return this.swl_Inventory;
    }

    public void setSwl_Inventory(String swl_Inventory) {
        this.swl_Inventory = swl_Inventory;
    }

    public String getAll_Inventory() {
        return this.all_Inventory;
    }

    public void setAll_Inventory(String all_Inventory) {
        this.all_Inventory = all_Inventory;
    }

    public String getTwo_groupName() {
        return this.two_groupName;
    }

    public void setTwo_groupName(String two_groupName) {
        this.two_groupName = two_groupName;
    }

    public String getThree_groupName() {
        return this.three_groupName;
    }

    public void setThree_groupName(String three_groupName) {
        this.three_groupName = three_groupName;
    }

    public String getGroupIDCopy() {
        return this.groupIDCopy;
    }

    public void setGroupIDCopy(String groupIDCopy) {
        this.groupIDCopy = groupIDCopy;
    }

    public String getThree_groupExtName() {
        return this.three_groupExtName;
    }

    public void setThree_groupExtName(String three_groupExtName) {
        this.three_groupExtName = three_groupExtName;
    }

    public String getGroupIDCombination() {
        return this.groupIDCombination;
    }

    public void setGroupIDCombination(String groupIDCombination) {
        this.groupIDCombination = groupIDCombination;
    }

    public String getCatgroup_all_cache() {
        return this.catgroup_all_cache;
    }

    public void setCatgroup_all_cache(String catgroup_all_cache) {
        this.catgroup_all_cache = catgroup_all_cache;
    }

    public String getArticlePoint() {
        return this.articlePoint;
    }

    public void setArticlePoint(String articlePoint) {
        this.articlePoint = articlePoint;
    }

    public String getCountOfarticle() {
        return this.countOfarticle;
    }

    public void setCountOfarticle(String countOfarticle) {
        this.countOfarticle = countOfarticle;
    }

    public String getPraiseRate() {
        return this.praiseRate;
    }

    public void setPraiseRate(String praiseRate) {
        this.praiseRate = praiseRate;
    }

    public String getListPraiseRate() {
        return this.listPraiseRate;
    }

    public void setListPraiseRate(String listPraiseRate) {
        this.listPraiseRate = listPraiseRate;
    }

    public String getListCommentScore() {
        return this.listCommentScore;
    }

    public void setListCommentScore(String listCommentScore) {
        this.listCommentScore = listCommentScore;
    }

    public String getListBrandScore() {
        return this.listBrandScore;
    }

    public void setListBrandScore(String listBrandScore) {
        this.listBrandScore = listBrandScore;
    }

    public String get_DY_price() {
        return this._DY_price;
    }

    public void set_DY_price(String _DY_price) {
        this._DY_price = _DY_price;
    }

    public String get_DY_swl_price() {
        return this._DY_swl_price;
    }

    public void set_DY_swl_price(String _DY_swl_price) {
        this._DY_swl_price = _DY_swl_price;
    }

    public String get_DY_best_price() {
        return this._DY_best_price;
    }

    public void set_DY_best_price(String _DY_best_price) {
        this._DY_best_price = _DY_best_price;
    }

    public String getSupplier_num() {
        return this.supplier_num;
    }

    public void setSupplier_num(String supplier_num) {
        this.supplier_num = supplier_num;
    }

    public String getPricing() {
        return this.pricing;
    }

    public void setPricing(String pricing) {
        this.pricing = pricing;
    }

    public String getDiscount() {
        return this.discount;
    }

    public void setDiscount(String discount) {
        this.discount = discount;
    }

    public String getCityIds() {
        return this.cityIds;
    }

    public void setCityIds(String cityIds) {
        this.cityIds = cityIds;
    }

    public String getSwl_cityIds() {
        return this.swl_cityIds;
    }

    public void setSwl_cityIds(String swl_cityIds) {
        this.swl_cityIds = swl_cityIds;
    }

    public String getAll_cityIds() {
        return this.all_cityIds;
    }

    public void setAll_cityIds(String all_cityIds) {
        this.all_cityIds = all_cityIds;
    }

    public String getAuthor() {
        return this.author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getPrintingDate() {
        return this.printingDate;
    }

    public void setPrintingDate(String printingDate) {
        this.printingDate = printingDate;
    }

    public String getMedia() {
        return this.media;
    }

    public void setMedia(String media) {
        this.media = media;
    }

    public String getIsbn() {
        return this.isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public String getBtcode() {
        return this.btcode;
    }

    public void setBtcode(String btcode) {
        this.btcode = btcode;
    }

    public String getBtsort() {
        return this.btsort;
    }

    public void setBtsort(String btsort) {
        this.btsort = btsort;
    }

    public String getBtgroup() {
        return this.btgroup;
    }

    public void setBtgroup(String btgroup) {
        this.btgroup = btgroup;
    }

    public String getUnit_searchable_attr() {
        return this.unit_searchable_attr;
    }

    public void setUnit_searchable_attr(String unit_searchable_attr) {
        this.unit_searchable_attr = unit_searchable_attr;
    }

    public String get_DYM_attrId() {
        return this._DYM_attrId;
    }

    public void set_DYM_attrId(String _DYM_attrId) {
        this._DYM_attrId = _DYM_attrId;
    }

    public String get_DYM_attr_unite() {
        return this._DYM_attr_unite;
    }

    public void set_DYM_attr_unite(String _DYM_attr_unite) {
        this._DYM_attr_unite = _DYM_attr_unite;
    }

    public String getGoodsType() {
        return this.goodsType;
    }

    public void setGoodsType(String goodsType) {
        this.goodsType = goodsType;
    }

    public String getGoodsOrder() {
        return this.goodsOrder;
    }

    public void setGoodsOrder(String goodsOrder) {
        this.goodsOrder = goodsOrder;
    }

    public String getMaternalOrder() {
        return this.maternalOrder;
    }

    public void setMaternalOrder(String maternalOrder) {
        this.maternalOrder = maternalOrder;
    }

    public String getCosmeticsOrder() {
        return this.cosmeticsOrder;
    }

    public void setCosmeticsOrder(String cosmeticsOrder) {
        this.cosmeticsOrder = cosmeticsOrder;
    }

    public String getImportBookOrder() {
        return this.importBookOrder;
    }

    public void setImportBookOrder(String importBookOrder) {
        this.importBookOrder = importBookOrder;
    }

    public String getMagazineOrder() {
        return this.magazineOrder;
    }

    public void setMagazineOrder(String magazineOrder) {
        this.magazineOrder = magazineOrder;
    }

    public String getCdOrder() {
        return this.cdOrder;
    }

    public void setCdOrder(String cdOrder) {
        this.cdOrder = cdOrder;
    }

    public String getMdmGroupId() {
        return this.mdmGroupId;
    }

    public void setMdmGroupId(String mdmGroupId) {
        this.mdmGroupId = mdmGroupId;
    }

    public String getContractInfos() {
        return this.contractInfos;
    }

    public void setContractInfos(String contractInfos) {
        this.contractInfos = contractInfos;
    }

    public String getPromotions() {
        return this.promotions;
    }

    public void setPromotions(String promotions) {
        this.promotions = promotions;
    }

    public String getActivity() {
        return this.activity;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    public String getEditGroupIDCopy() {
        return this.editGroupIDCopy;
    }

    public void setEditGroupIDCopy(String editGroupIDCopy) {
        this.editGroupIDCopy = editGroupIDCopy;
    }

    public String getEditGroupIDCombination() {
        return this.editGroupIDCombination;
    }

    public void setEditGroupIDCombination(String editGroupIDCombination) {
        this.editGroupIDCombination = editGroupIDCombination;
    }

    public String getIsNew() {
        return this.isNew;
    }

    public void setIsNew(String isNew) {
        this.isNew = isNew;
    }

    public String getFind_source() {
        return this.find_source;
    }

    public void setFind_source(String find_source) {
        this.find_source = find_source;
    }

    public String getIs_vaild_citys() {
        return this.is_vaild_citys;
    }

    public void setIs_vaild_citys(String is_vaild_citys) {
        this.is_vaild_citys = is_vaild_citys;
    }

    public String get_DY_activityBeginTime() {
        return this._DY_activityBeginTime;
    }

    public void set_DY_activityBeginTime(String _DY_activityBeginTime) {
        this._DY_activityBeginTime = _DY_activityBeginTime;
    }

    public String get_DY_activityEndTime() {
        return this._DY_activityEndTime;
    }

    public void set_DY_activityEndTime(String _DY_activityEndTime) {
        this._DY_activityEndTime = _DY_activityEndTime;
    }

    public String get_DC_SOLR_SHARD() {
        return this._DC_SOLR_SHARD;
    }

    public void set_DC_SOLR_SHARD(String _DC_SOLR_SHARD) {
        this._DC_SOLR_SHARD = _DC_SOLR_SHARD;
    }

    public String get_DC_PRODUCT_TYPE() {
        return this._DC_PRODUCT_TYPE;
    }

    public void set_DC_PRODUCT_TYPE(String _DC_PRODUCT_TYPE) {
        this._DC_PRODUCT_TYPE = _DC_PRODUCT_TYPE;
    }

    public String get_DC_DEL_FLAG() {
        return this._DC_DEL_FLAG;
    }

    public void set_DC_DEL_FLAG(String _DC_DEL_FLAG) {
        this._DC_DEL_FLAG = _DC_DEL_FLAG;
    }

    public String getSupre_sale_type() {
        return this.supre_sale_type;
    }

    public void setSupre_sale_type(String supre_sale_type) {
        this.supre_sale_type = supre_sale_type;
    }

    public String getChannel() {
        return this.channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getComprehensiveSortScores() {
        return this.comprehensiveSortScores;
    }

    public void setComprehensiveSortScores(String comprehensiveSortScores) {
        this.comprehensiveSortScores = comprehensiveSortScores;
    }

    public String getClickCount() {
        return this.clickCount;
    }

    public void setClickCount(String clickCount) {
        this.clickCount = clickCount;
    }

    public String getSaleCount() {
        return this.saleCount;
    }

    public void setSaleCount(String saleCount) {
        this.saleCount = saleCount;
    }

    public String getSalesAmount() {
        return this.salesAmount;
    }

    public void setSalesAmount(String salesAmount) {
        this.salesAmount = salesAmount;
    }

    public String getCommodityFlag() {
        return this.commodityFlag;
    }

    public void setCommodityFlag(String commodityFlag) {
        this.commodityFlag = commodityFlag;
    }

    public String getMainCommodityId() {
        return this.mainCommodityId;
    }

    public void setMainCommodityId(String mainCommodityId) {
        this.mainCommodityId = mainCommodityId;
    }

    public String getBrand_homeStream() {
        return this.brand_homeStream;
    }

    public void setBrand_homeStream(String brand_homeStream) {
        this.brand_homeStream = brand_homeStream;
    }

    public String getGoods_gender() {
        return this.goods_gender;
    }

    public void setGoods_gender(String goods_gender) {
        this.goods_gender = goods_gender;
    }

    public String get_DC_UPDATE_TIME() {
        return this._DC_UPDATE_TIME;
    }

    public void set_DC_UPDATE_TIME(String _DC_UPDATE_TIME) {
        this._DC_UPDATE_TIME = _DC_UPDATE_TIME;
    }

    public String toString()
    {
        return this.partnumber + "," + this.catentry_Id + "," + this.catalog + "," + this.catentdesc + "," + this.title + "," + this.brand_Name + "," + this.brand_Name_FacetAll + "," + this.virtual_Name + "," + this.description + "," + this.auxdescription + "," + this.availabilitydate + "," + this.totalCount + "," + this.totalCountScore + "," + this.inventoryScore + "," + this.inventory + "," + this.swl_Inventory + "," + this.all_Inventory + "," + this.groupIDCopy + "," + this.groupIDCombination + "," + this.catgroup_all_cache + "," + this.articlePoint + "," + this.countOfarticle + "," + this.praiseRate + "," + this._DY_price + "," + this._DY_swl_price + "," + this._DY_best_price + "," + this.supplier_num + "," + this.discount + "," + this.cityIds + "," + this.swl_cityIds + "," + this.all_cityIds + "," + this.author + "," + this.printingDate + "," + this.isbn + "," + this.unit_searchable_attr + "," + this.goodsType + "," + this.goodsOrder + "," + this.maternalOrder + "," + this.cosmeticsOrder + "," + this.mdmGroupId + "," + this.editGroupIDCopy + "," + this.editGroupIDCombination + "," + this.isNew + "," + this.find_source + "," + this.is_vaild_citys + "," + this._DC_SOLR_SHARD + "," + this._DC_PRODUCT_TYPE + "," + this._DC_DEL_FLAG + "," + this.comprehensiveSortScores + "," + this._DC_UPDATE_TIME + "," + this.categoryId;
    }
}