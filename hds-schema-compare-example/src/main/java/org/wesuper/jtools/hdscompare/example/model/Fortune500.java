package org.wesuper.jtools.hdscompare.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

/**
 * Fortune500公司信息实体类
 */
public class Fortune500 {
    @JsonProperty("id")
    private Long id;

    @JsonProperty("rank")
    private Long rank;

    @JsonProperty("company_name")
    private String companyName;

    @JsonProperty("country")
    private String country;

    @JsonProperty("employees_num")
    private Long employeesNum;

    @JsonProperty("employees_num_change_percentage")
    private Double employeesNumChangePercentage;

    @JsonProperty("previous_rank")
    private Long previousRank;

    @JsonProperty("revenues_million")
    private Long revenuesMillion;

    @JsonProperty("revene_change_percentage")
    private Double reveneChangePercentage;

    @JsonProperty("profit_million")
    private Double profitMillion;

    @JsonProperty("profit_change_percentage")
    private Double profitChangePercentage;

    @JsonProperty("asset_million")
    private Long assetMillion;

    @JsonProperty("asset_change_percentage")
    private Double assetChangePercentage;

    @JsonProperty("year")
    private Long year;

    @JsonProperty("modify_at")
    private Date modifyAt;

    @JsonProperty("location")
    private String location;

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getRank() {
        return rank;
    }

    public void setRank(Long rank) {
        this.rank = rank;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Long getEmployeesNum() {
        return employeesNum;
    }

    public void setEmployeesNum(Long employeesNum) {
        this.employeesNum = employeesNum;
    }

    public Double getEmployeesNumChangePercentage() {
        return employeesNumChangePercentage;
    }

    public void setEmployeesNumChangePercentage(Double employeesNumChangePercentage) {
        this.employeesNumChangePercentage = employeesNumChangePercentage;
    }

    public Long getPreviousRank() {
        return previousRank;
    }

    public void setPreviousRank(Long previousRank) {
        this.previousRank = previousRank;
    }

    public Long getRevenuesMillion() {
        return revenuesMillion;
    }

    public void setRevenuesMillion(Long revenuesMillion) {
        this.revenuesMillion = revenuesMillion;
    }

    public Double getReveneChangePercentage() {
        return reveneChangePercentage;
    }

    public void setReveneChangePercentage(Double reveneChangePercentage) {
        this.reveneChangePercentage = reveneChangePercentage;
    }

    public Double getProfitMillion() {
        return profitMillion;
    }

    public void setProfitMillion(Double profitMillion) {
        this.profitMillion = profitMillion;
    }

    public Double getProfitChangePercentage() {
        return profitChangePercentage;
    }

    public void setProfitChangePercentage(Double profitChangePercentage) {
        this.profitChangePercentage = profitChangePercentage;
    }

    public Long getAssetMillion() {
        return assetMillion;
    }

    public void setAssetMillion(Long assetMillion) {
        this.assetMillion = assetMillion;
    }

    public Double getAssetChangePercentage() {
        return assetChangePercentage;
    }

    public void setAssetChangePercentage(Double assetChangePercentage) {
        this.assetChangePercentage = assetChangePercentage;
    }

    public Long getYear() {
        return year;
    }

    public void setYear(Long year) {
        this.year = year;
    }

    public Date getModifyAt() {
        return modifyAt;
    }

    public void setModifyAt(Date modifyAt) {
        this.modifyAt = modifyAt;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
} 