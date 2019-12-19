package com.xgit.xghb.bean;

/**
 * @program: onLineTranspondProject
 * @description: 请求数据Body载体
 * @author: Mr.Wang
 * @create: 2019-11-07 13:40
 **/
public class Body {
    /**
     * 数据采集时间  已获取，为GPS时间
     */
    private String SJCJSCJ;

    /**
     * 终端对应的车辆ID(从环保局库 nrmm_equipment_info 的ID中获取)
     */
    private String hbclId;
    /**
     * 机械环保序号
     */
    private String JXHBXH;
    /**
     * Sim卡号
     */
    private String SIMID;
    /**
     * 终端ID  已获取
     */
    private String ZDID;
    /**
     * 定位状态  已获取
     */
    private String DWZT;
    /**
     * 经度    已获取
     */
    private String JD;
    /**
     * 纬度    已获取
     */
    private String WD;
    /**
     * 海拔     已获取
     */
    private String HB;
    /**
     * 车速     已接受
     */
    private String CS;
    /**
     * ACC状态    已获取
     */
    private String ACCZT;
    /**
     * ACC累计工作时长    已获取
     */
    private String ACCLJGZSC;
    /**
     * 报警组合信息
     */
    //TODO 报警组合信息未获取
    private String BJZHXX;

    public String getSJCJSCJ() {
        return SJCJSCJ;
    }

    public void setSJCJSCJ(String SJCJSCJ) {
        this.SJCJSCJ = SJCJSCJ;
    }

    public String getJXHBXH() {
        return JXHBXH;
    }

    public void setJXHBXH(String JXHBXH) {
        this.JXHBXH = JXHBXH;
    }

    public String getSIMID() {
        return SIMID;
    }

    public void setSIMID(String SIMID) {
        this.SIMID = SIMID;
    }

    public String getZDID() {
        return ZDID;
    }

    public void setZDID(String ZDID) {
        this.ZDID = ZDID;
    }

    public String getDWZT() {
        return DWZT;
    }

    public void setDWZT(String DWZT) {
        this.DWZT = DWZT;
    }

    public String getJD() {
        return JD;
    }

    public void setJD(String JD) {
        this.JD = JD;
    }

    public String getWD() {
        return WD;
    }

    public void setWD(String WD) {
        this.WD = WD;
    }

    public String getHB() {
        return HB;
    }

    public void setHB(String HB) {
        this.HB = HB;
    }

    public String getCS() {
        return CS;
    }

    public void setCS(String CS) {
        this.CS = CS;
    }

    public String getACCZT() {
        return ACCZT;
    }

    public void setACCZT(String ACCZT) {
        this.ACCZT = ACCZT;
    }

    public String getACCLJGZSC() {
        return ACCLJGZSC;
    }

    public void setACCLJGZSC(String ACCLJGZSC) {
        this.ACCLJGZSC = ACCLJGZSC;
    }

    public String getBJZHXX() {
        return BJZHXX;
    }

    public void setBJZHXX(String BJZHXX) {
        this.BJZHXX = BJZHXX;
    }

    public String getHbclId() {
        return hbclId;
    }

    public void setHbclId(String hbclId) {
        this.hbclId = hbclId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"SJCJSCJ\":\"")
                .append(SJCJSCJ).append('\"');
        sb.append(",\"hbclId\":\"")
                .append(hbclId).append('\"');
        sb.append(",\"JXHBXH\":\"")
                .append(JXHBXH).append('\"');
        sb.append(",\"SIMID\":\"")
                .append(SIMID).append('\"');
        sb.append(",\"ZDID\":\"")
                .append(ZDID).append('\"');
        sb.append(",\"DWZT\":\"")
                .append(DWZT).append('\"');
        sb.append(",\"JD\":\"")
                .append(JD).append('\"');
        sb.append(",\"WD\":\"")
                .append(WD).append('\"');
        sb.append(",\"HB\":\"")
                .append(HB).append('\"');
        sb.append(",\"CS\":\"")
                .append(CS).append('\"');
        sb.append(",\"ACCZT\":\"")
                .append(ACCZT).append('\"');
        sb.append(",\"ACCLJGZSC\":\"")
                .append(ACCLJGZSC).append('\"');
        sb.append(",\"BJZHXX\":\"")
                .append(BJZHXX).append('\"');
        sb.append('}');
        return sb.toString();
    }
}
