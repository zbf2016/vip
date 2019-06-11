package com.midea.cmvip.core.service;

import com.midea.ccrm.user.api.UserVipInfoDaoFacade;
import com.midea.ccrm.user.api.dto.VipUser;
import com.midea.ccrm.user.api.dto.VipUserInfoDTO;
import com.midea.ccrm.user.api.dto.entity.VipGiftRuleRecord;
import com.midea.ccrm.user.api.dto.request.VipGiftRuleReq;
import com.midea.cmms.base.Result;
import com.midea.cmms.base.redis.RedisLock;
import com.midea.cmvip.core.CacheConst;
import com.midea.cmvip.core.api.GiftBagSendServiceFacade;
import com.midea.cmvip.core.api.dto.req.GrantGiftBagReq;
import com.midea.cmvip.core.api.dto.req.ReceiveCouponReq;
import com.midea.cmvip.core.api.dto.resp.*;
import com.midea.cmvip.core.api.enums.BrandEnum;
import com.midea.cmvip.core.dao.mapper.CouponRuleMapper;
import com.midea.cmvip.core.dao.mapper.CustomRuleMapper;
import com.midea.cmvip.core.dao.mapper.GiftRuleLevelMapper;
import com.midea.cmvip.core.dao.mapper.GiftRuleMapper;
import com.midea.cmvip.core.entity.*;
import com.midea.cmvip.core.entity.GiftRule;
import com.midea.cmvip.core.utils.CheckMoblieUtil;
import com.midea.cmvip.core.utils.FUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author zhangbf
 * @date 2018-8-28
 * Description:
 */
@Transactional
public class GiftBagSendService implements GiftBagSendServiceFacade {
    private static Logger logger = LoggerFactory.getLogger(GiftBagSendService.class);

    @Autowired
    CustomRuleMapper customRuleMapper;

    @Autowired
    private UserVipInfoDaoFacade userVipInfoDaoFacade;

    @Autowired
    GiftRuleMapper giftRuleMapper;

    @Autowired
    CouponRuleMapper couponRuleMapper;

    @Autowired
    GiftRuleLevelMapper giftRuleLevelMapper;

    @Autowired
    LevelRuleService levelRuleService;

    @Autowired
    private RedisLock redisLock;

    private static long ONE = 1;//使用数


    @Override
    public Result sendCustService() throws Exception {
        logger.info("===>发送客户服务开始<====");
        Result result = new Result();
        List<CustomRule> customRules = customRuleMapper.selectByExample(new CustomRuleExample());
        VipGradeListResp resp = new VipGradeListResp();
        List<VipGradeResp> vip = new ArrayList<>();
        for (CustomRule rule : customRules) {
            GiftRuleLevelExample example = new GiftRuleLevelExample();
            example.createCriteria().andCustIdEqualTo(rule.getCustId());
            List<GiftRuleLevel> levels = giftRuleLevelMapper.selectByExample(example);
            for (GiftRuleLevel level : levels) {
                VipGradeResp grade = new VipGradeResp();
                grade.setLevelName(level.getLevelName());
                grade.setLevelId(level.getLevelId());
                vip.add(grade);
            }
            resp.setDescription(rule.getDescription());
        }
        resp.setVipGrades(vip);
        result.setData(resp);
        result.setMsg("查询成功");
        return result;
    }

    @Override
    public Result sendUser(String moblie) throws Exception {
        logger.info("===>根据手机号查询会员信息，手机号为{}<===", moblie);
        Result result = new Result();
        result.setMsg("查询失败");
        if (CheckMoblieUtil.isMobile(moblie)) {
            VipUser vipUser = userVipInfoDaoFacade.findUserByBrandAndMobile(BrandEnum.MIDEA.getBrandId(), moblie);
            VipUserInfoDTO infoDTO = vipUser.getVipUserInfoDTO();

            VipUserResp vip = new VipUserResp();
            vip.setMoblie(moblie);
            Result<LevelRuleDTO> rule = levelRuleService.getByLevelId(infoDTO.getVipLevel());
            LevelRuleDTO dto = (LevelRuleDTO) rule.getData();
            vip.setVipLevel(dto.getLevelName());
            vip.setVipUserName(vipUser.getName());
            result.setMsg("查询成功");
            result.setData(vip);
            return result;
        }
        return result;
    }

    @Override
    public Result getCoupon(GrantGiftBagReq req) throws Exception {
        Result result = new Result();
      /*  RedisLock.Lock lock = lockInvokeRule(req.getUid());
        try {*/
            //1.查询用户是否存在
            VipUser vipUser = null;
            if (StringUtils.isNotBlank(req.getUid())) {
                vipUser = userVipInfoDaoFacade.findUserByUID(req.getBrand(),req.getUid());
            }
            if (StringUtils.isBlank(req.getUid()) && StringUtils.isNotBlank(req.getMobile()) && req.getBrand() != null) {
                vipUser = userVipInfoDaoFacade.findUserByBrandAndMobile(req.getBrand(), req.getMobile());
                //uid为空，表示不是会员
                if (StringUtils.isBlank(vipUser.getVipUserInfoDTO().getUid())) {
                    vipUser = null;
                }
            }
            if (vipUser == null) {
                logger.info("uid为{}，或者手机号为{}，品牌为{}，不是会员，流程结束", req.getUid(), req.getMobile(), req.getBrand());
                result.setMsg("该手机号以及品牌或者uid不是会员");
                result.setCode(-1);
                return result;
            }
            //2.查询礼包
            GiftRule giftRule = null;
            long stock = 0;
            if (StringUtils.isNotBlank(req.getGiftId())) {
                //2.1 查询礼包是否存在
                GiftRuleExample example = new GiftRuleExample();
                example.createCriteria().andGiftIdEqualTo(req.getGiftId());
                List<GiftRule> rules = giftRuleMapper.selectByExample(example);
                if (CollectionUtils.isNotEmpty(rules)) {
                    giftRule = rules.get(0);
                }
                //2.2 查询礼包是否启用
                if (giftRule == null || giftRule.getEnable() != 1) {
                    logger.info("该礼包id没有礼包，礼包id为{},或者该礼包没有启用，礼包状态为{}", req.getGiftId(), giftRule.getEnable());
                    result.setMsg("该礼包id没有礼包,或者该礼包没有启用");
                    result.setCode(-1);
                    return result;
                }
                //2.3 查询礼包的有效期
                if (giftRule.getValidType() == 1) {
                    if (!(giftRule.getStartDate().getTime() <= new Date().getTime() && new Date().getTime() <= giftRule.getEndDate().getTime())) {
                        logger.info("当前时间不在该礼包有效期内，有效期开始时间为{}，有效期结束时间为{}", giftRule.getStartDate(), giftRule.getEndDate());
                        result.setMsg("当前时间不在该礼包有效期内");
                        result.setCode(-1);
                        return result;
                    }
                }
                //2.4 查询礼包库存
                List<CouponRule> couponRuleList = getCouponRule(req.getGiftId());
                if (CollectionUtils.isNotEmpty(couponRuleList)) {
                    stock = couponRuleList.get(0).getStock();
                }
                if (stock <= 0) {
                    logger.info("该礼包库存为0，结束流程");
                    result.setMsg("该礼包库存为0，结束流程");
                    result.setCode(-1);
                    return result;
                }
                //2.5 查询礼包对应会员等级
                if (StringUtils.isNotBlank(vipUser.getVipUserInfoDTO().getVipLevel())) {
                    boolean flag = false;
                    GiftRuleLevelExample levelExample = new GiftRuleLevelExample();
                    levelExample.createCriteria().andGiftIdEqualTo(req.getGiftId());
                    List<GiftRuleLevel> giftRuleLevels = giftRuleLevelMapper.selectByExample(levelExample);
                    String gradeName = "";
                    for (GiftRuleLevel level : giftRuleLevels) {
                        gradeName += level.getLevelName() + ",";
                        if (vipUser.getVipUserInfoDTO().getVipLevel().equals(level.getLevelId())) {
                            flag = true;
                        }
                    }
                    if (!flag) {
                        logger.info("该会员的等级，不能享受该礼包，该会员等级为{}，该礼包包含的会员等级为{}", vipUser.getVipUserInfoDTO().getVipLevel(), gradeName);
                        result.setMsg("该会员的等级，不能享受该礼包");
                        result.setCode(-1);
                        return result;
                    }
                }
            }
            //3. 查询用户是否发过礼包
            VipGiftRuleReq ruleReq = new VipGiftRuleReq();
            ruleReq.setCouponType(giftRule != null ? giftRule.getCouponType() : "");
            ruleReq.setUserId(vipUser.getVipUserInfoDTO().getUserId());
            ruleReq.setGiftId(req.getGiftId());
            ruleReq.setUid(req.getUid());
            ruleReq.setMoblie(req.getMobile());
            ruleReq.setUserId(vipUser.getVipUserInfoDTO().getUserId());
            ruleReq.setGradeLevel(vipUser.getVipUserInfoDTO().getVipLevel());
            List<VipGiftRuleRecord> vipGiftRuleRecord = userVipInfoDaoFacade.queryVipUserGiftBag(ruleReq);
            if (CollectionUtils.isNotEmpty(vipGiftRuleRecord)) {
                logger.info("该用户已发放过礼包，礼包id为{}，uid为{}", req.getGiftId(), req.getUid());
                result.setMsg("该用户已发放过礼包");
                result.setCode(-1);
                return result;
            }
            //4.扣减库存
            stock--;
            List<CouponRule> couponRuleList = getCouponRule(req.getGiftId());
            //String convertCode = "";
            List<VipGiftRuleRecord> recordList = new ArrayList<>();
            for (CouponRule rule : couponRuleList) {
                CouponRule coupon = new CouponRule();
                BeanUtils.copyProperties(rule, coupon);
                coupon.setStock(stock);
                coupon.setUseStock(rule.getUseStock() == null ? ONE : rule.getUseStock() + ONE);
                couponRuleMapper.updateByPrimaryKeySelective(coupon);
                //convertCode += convertCode + ",";
                VipGiftRuleRecord record = new VipGiftRuleRecord();
                record.setGiftId(req.getGiftId());
                record.setUserId(vipUser.getVipUserInfoDTO().getUserId());
                record.setCouponId(rule.getId());
                record.setUid(req.getUid());
                record.setMobile(req.getMobile());
                record.setGradeLevel(vipUser.getVipUserInfoDTO().getVipLevel());
                record.setUseStock(ONE);
                record.setStock(stock);//库存
                record.setUseTime(new Date());//使用时间
                record.setCouponType(giftRule.getCouponType());
                record.setCouponName(rule.getCouponName());
                recordList.add(record);
            }
            //5.发送礼包，将礼包里优惠券兑换码返给电商
            result.setData(couponRuleList);
            //6.插入明细
            boolean success = userVipInfoDaoFacade.insertVipUserGiftBag(recordList, true);
            if (success) {
                logger.info("保存明细成功");
            }
       /* } finally {
            lock.unlock();
        }*/
        return result;
    }

    private RedisLock.Lock lockInvokeRule(String uid) {
        logger.info("进入锁，uid为{}", uid);
        RedisLock.Lock lock = redisLock.getLock(CacheConst.lockInvokeRule(uid));
        try {
            lock.lock(-1, 100);
        } catch (InterruptedException e) {
            throw new InternalError(e.getMessage());
        }
        return lock;
    }

    private List<CouponRule> getCouponRule(String giftId) {
        CouponRuleExample ruleExample = new CouponRuleExample();
        ruleExample.createCriteria().andGiftIdEqualTo(giftId);
        return couponRuleMapper.selectByExample(ruleExample);
    }

    @Override
    public Result getCouponStatus(GrantGiftBagReq req) throws Exception {
        logger.info("==>查询用户是否领取了礼包 <==");
        Result result = new Result();
        //1.查询用户是否存在
        VipUser vipUser = null;
        if (StringUtils.isNotBlank(req.getUid())) {
            vipUser = userVipInfoDaoFacade.findUserByUID(req.getBrand(),req.getUid());
        }
        if (StringUtils.isNotBlank(req.getMobile()) && req.getBrand() != null) {
            vipUser = userVipInfoDaoFacade.findUserByBrandAndMobile(req.getBrand(), req.getMobile());
            //uid为空，表示不是会员
            if (StringUtils.isBlank(vipUser.getVipUserInfoDTO().getUid())) {
                vipUser = null;
            }
        }
        if (vipUser == null) {
            result.setMsg("该手机号以及品牌或者uid不是会员");
            logger.info("uid为{}，或者手机号为{}，品牌为{}，不是会员，流程结束", req.getUid(), req.getMobile(), req.getBrand());
            result.setCode(-1);
            return result;
        }
        GiftRuleExample example = new GiftRuleExample();
        example.createCriteria().andGiftIdEqualTo(req.getGiftId());
        List<GiftRule> rules = giftRuleMapper.selectByExample(example);
        GiftRule giftRule = null;
        if (CollectionUtils.isNotEmpty(rules)) {
            giftRule = rules.get(0);
        }
        VipGiftRuleReq ruleReq = new VipGiftRuleReq();
        ruleReq.setCouponType(giftRule != null ? giftRule.getCouponType() : "");
        ruleReq.setUserId(vipUser.getVipUserInfoDTO().getUserId());
        ruleReq.setGiftId(req.getGiftId());
        ruleReq.setUid(req.getUid());
        ruleReq.setMoblie(req.getMobile());
        ruleReq.setGradeLevel(vipUser.getVipUserInfoDTO().getVipLevel());
        List<VipGiftRuleRecord> vipGiftRuleRecord = userVipInfoDaoFacade.queryVipUserGiftBag(ruleReq);
        VipReceiveResp resp = new VipReceiveResp();
        resp.setGiftName(giftRule.getGiftName());
        resp.setGiftType(giftRule.getGiftType() + "");
        if (CollectionUtils.isNotEmpty(vipGiftRuleRecord)) {
            resp.setIsReceive(1);
            result.setMsg("已领取");
        } else {
            resp.setIsReceive(0);
            result.setMsg("未领取");
        }
        result.setData(resp);
        return result;
    }

    @Override
    public Result receiveCoupon(ReceiveCouponReq req) throws Exception {
        logger.info("==>电商回调接口开始 <==");
        Result result = new Result();
      /*  RedisLock.Lock lock = lockInvokeRule(req.getUid());
        try {*/
            VipGiftRuleReq ruleReq = new VipGiftRuleReq();
            ruleReq.setGiftId(req.getGiftId());
            ruleReq.setUid(req.getUid());
            ruleReq.setMoblie(req.getMobile());
            ruleReq.setCouponId(req.getCouponId());
            List<VipGiftRuleRecord> vipGiftRuleRecord = userVipInfoDaoFacade.queryVipUserGiftBag(ruleReq);
            if (CollectionUtils.isEmpty(vipGiftRuleRecord)) {
                logger.info("没有查询到明细");
                result.setMsg("没有查询到发放明细");
                return result;
            }
            List<VipGiftRuleRecord> recordList = new ArrayList<>();
            VipGiftRuleRecord record = new VipGiftRuleRecord();
            if (req.getCouponNumber() != null) {
                record.setCouponNumber(req.getCouponNumber());
            }
            if (StringUtils.isNotBlank(req.getCouponUrl())) {
                record.setCouponUrl(req.getCouponUrl());
            }
            if (StringUtils.isNotBlank(req.getCouponPassword())) {
                record.setCouponPassword(req.getCouponPassword());
            }
            if (req.getWriteTime() != null) {
                record.setWriteTime(req.getWriteTime());
            }
            if (req.getCouponId() != null) {
                record.setCouponId(req.getCouponId());
            }
            if (req.getWriteStock() != null) {
                record.setWriteStock(req.getWriteStock());
            }
            record.setId(vipGiftRuleRecord.get(0).getId());
            recordList.add(record);
            boolean success = userVipInfoDaoFacade.insertVipUserGiftBag(recordList, false);
            if (success) {
                logger.info("保存明细成功：明细为{}", FUtil.toJson(record));
            }
            result.setMsg("设置成功");
      /*  } finally {
            lock.unlock();
        }*/
        return result;
    }

}
