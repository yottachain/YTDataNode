package account

import (
	"encoding/json"
	"fmt"
	"github.com/eoscanada/eos-go"
	"github.com/spf13/cobra"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/transaction"
	"strings"
)

//var baseNodeUrl = "http://dnapi1.yottachain.net:8888" //正式

var cfg *config.Config
var baseNodeUrl string
var api *eos.API
var opt eos.TxOptions

var AccountCmd = &cobra.Command{
	Use:   "account",
	Short: "账号管理",
}

type PoolInfo []struct {
	PoolID    string `json:"pool_id"`
	PoolOwner string `json:"pool_owner"`
	MaxSpace  uint64 `json:"max_space"`
}

func getPoolInfo(poolID string) (PoolInfo, error) {
	out, err := api.GetTableRows(eos.GetTableRowsRequest{
		Code:       "hddpool12345",
		Scope:      "hddpool12345",
		Table:      "storepool",
		Index:      "1",
		Limit:      1,
		LowerBound: poolID,
		UpperBound: poolID,
		JSON:       true,
		KeyType:    "name",
	})
	if err != nil {
		return nil, err
	}
	var res PoolInfo
	json.Unmarshal(out.Rows, &res)
	return res, nil
}

var changeAdminCmd = &cobra.Command{
	Use:   "change-admin",
	Short: "更改矿机管理员",
	Run: func(cmd *cobra.Command, args []string) {
		ad := &struct {
			Minerid     uint64          `json:"minerid"`
			NewAdminacc eos.AccountName `json:"new_adminacc" prompt:"请输入新的矿机管理员账号" required:"true"`
		}{
			Minerid: uint64(cfg.IndexID),
		}
		request, err := transaction.NewSignedTransactionRequest(
			ad,
			"hddpool12345",
			"mchgadminacc",
			[]eos.PermissionLevel{
				eos.PermissionLevel{
					eos.AN(cfg.Adminacc),
					"active",
				},
			},
			&opt,
		)
		if err != nil {
			fmt.Println(err)
		} else {
			res, err := request.Send(cfg.GetAPIAddr() + "/ChangeAdminAcc")
			if err != nil {
				fmt.Println("操作失败:", err, string(res))
				return
			}
			cfg.Adminacc = string(ad.NewAdminacc)
			cfg.Save()
			fmt.Printf("操作成功，矿机管理员替换为:%s\n", ad.NewAdminacc)
		}
	},
}

var changeOwnerCmd = &cobra.Command{
	Use:   "change-owner",
	Short: "更改收益账号",
	Run: func(cmd *cobra.Command, args []string) {
		ad := &struct {
			Minerid     uint64          `json:"minerid"`
			NewOwnerAcc eos.AccountName `json:"new_owneracc" prompt:"请输入新的矿机收益账号" required:"true"`
		}{
			Minerid: uint64(cfg.IndexID),
		}

		info, err := getPoolInfo(cfg.PoolID)
		if err != nil {
			fmt.Println("操作失败:", err)
			return
		}

		p := []eos.PermissionLevel{
			eos.PermissionLevel{
				eos.AN(cfg.Adminacc),
				"active",
			},
			eos.PermissionLevel{
				eos.AN(info[0].PoolOwner),
				"active",
			},
		}
		request, err := transaction.NewSignedTransactionRequest(
			ad,
			"hddpool12345",
			"mchgowneracc",
			p,
			&opt,
		)
		if err != nil {
			fmt.Println(err)
		} else {
			res, err := request.Send(cfg.GetAPIAddr() + "/ChangeProfitAcc")
			if err != nil {
				fmt.Println("操作失败:", err, string(res))
				return
			}
			fmt.Printf("操作成功，矿机收益人替换为:%s\n", ad.NewOwnerAcc)
		}
	},
}

var changePoolIDCmd = &cobra.Command{
	Use:   "change-pool-id",
	Short: "更改矿池ID",
	Run: func(cmd *cobra.Command, args []string) {
		ad := &struct {
			Minerid   uint64          `json:"minerid"`
			NewPoolID eos.AccountName `json:"new_poolid" prompt:"请输入新的矿池ID" required:"true"`
		}{
			Minerid: uint64(cfg.IndexID),
		}

		info, err := getPoolInfo(cfg.PoolID)
		if err != nil {
			fmt.Println("操作失败:", err)
			return
		}

		p := []eos.PermissionLevel{
			eos.PermissionLevel{
				eos.AN(cfg.Adminacc),
				"active",
			},
			eos.PermissionLevel{
				eos.AN(info[0].PoolOwner),
				"active",
			},
		}
		request, err := transaction.NewSignedTransactionRequest(
			ad,
			"hddpool12345",
			"mchgstrpool",
			p,
			&opt,
		)
		if err != nil {
			fmt.Println(err)
		} else {
			res, err := request.Send(cfg.GetAPIAddr() + "/ChangePoolID")
			if err != nil {
				fmt.Println("操作失败:", err, string(res))
				return
			}
			cfg.PoolID = string(ad.NewPoolID)
			cfg.Save()
			fmt.Printf("操作成功，矿池ID替换为:%s\n", ad.NewPoolID)
		}
	},
}

var changeMaxSpaceCmd = &cobra.Command{
	Use:   "change-max-space",
	Short: "更改最大可采购空间",
	Run: func(cmd *cobra.Command, args []string) {
		ad := &struct {
			Minerid  uint64 `json:"minerid"`
			MaxSpace uint64 `json:"max_space" prompt:"请输入最大可采购空间 单位：G 最小100G，最大100T）" convert:"Block" required:"true"`
		}{
			Minerid: uint64(cfg.IndexID),
		}

		info, err := getPoolInfo(cfg.PoolID)
		if err != nil {
			fmt.Println("操作失败:", err)
			return
		}

		p := []eos.PermissionLevel{
			eos.PermissionLevel{
				eos.AN(cfg.Adminacc),
				"active",
			},
			eos.PermissionLevel{
				eos.AN(info[0].PoolOwner),
				"active",
			},
		}
		request, err := transaction.NewSignedTransactionRequest(
			ad,
			"hddpool12345",
			"mchgspace",
			p,
			&opt,
		)
		if err != nil {
			fmt.Println(err)
		} else {
			res, err := request.Send(cfg.GetAPIAddr() + "/ChangeAssignedSpace")
			if err != nil {
				fmt.Println("pool", eos.AN(info[0].PoolOwner))
				fmt.Println("操作失败:", err, string(res))
				return
			}
			fmt.Printf("操作成功，最大可采供空间更改为:%d\n", ad.MaxSpace)
		}
	},
}

var changeDepAccCmd = &cobra.Command{
	Use:   "change-dep-acc",
	Short: "更改抵押账户",
	Run: func(cmd *cobra.Command, args []string) {
		ad := &struct {
			Minerid   uint64          `json:"minerid"`
			NewDepAcc eos.AccountName `json:"new_depacc" prompt:"请输入新的抵押账户" required:"true"`
		}{
			Minerid: uint64(cfg.IndexID),
		}

		info, err := getPoolInfo(cfg.PoolID)
		if err != nil {
			fmt.Println("操作失败:", err)
			return
		}

		p := []eos.PermissionLevel{
			eos.PermissionLevel{
				eos.AN(cfg.Adminacc),
				"active",
			},
			eos.PermissionLevel{
				eos.AN(info[0].PoolOwner),
				"active",
			},
		}
		request, err := transaction.NewSignedTransactionRequest(
			ad,
			"hdddeposit12",
			"mchgdepacc",
			p,
			&opt,
		)
		if err != nil {
			fmt.Println(err)
		} else {
			res, err := request.Send(cfg.GetAPIAddr() + "/ChangeDepAcc")
			if err != nil {
				fmt.Println("操作失败:", err, string(res))
				return
			}
			fmt.Printf("操作成功，更改抵押账户成功:%s\n", ad.NewDepAcc)
		}
	},
}

var changeDepositCmd = &cobra.Command{
	Use:   "change-deposit",
	Short: "更改抵押金额",
	Run: func(cmd *cobra.Command, args []string) {
		var valuestring string
	input:
		fmt.Println("请输入当前抵押账号")
		fmt.Scanln(&valuestring)
		if valuestring == "" {
			goto input
		}

		ad := &struct {
			User       eos.AccountName `json:"user" require:"true"`
			Minerid    uint64          `json:"minerid"`
			IsIncrease bool            `json:"is_increase" prompt:"是否为追加抵押（yes 或者 no）" required:"true"`
			Quant      eos.Asset       `json:"quant" prompt:"请输入增加额度" require:"true"`
		}{
			User:    eos.AN(valuestring),
			Minerid: uint64(cfg.IndexID),
		}

		//info, err := getPoolInfo(cfg.PoolID)
		//if err != nil {
		//	fmt.Println("操作失败:", err)
		//	return
		//}

		p := []eos.PermissionLevel{
			eos.PermissionLevel{
				eos.AN(valuestring),
				"active",
			},
			//eos.PermissionLevel{
			//	eos.AN(info[0].PoolOwner),
			//	"owner",
			//},
		}

		request, err := transaction.NewSignedTransactionRequest(
			ad,
			"hdddeposit12",
			"chgdeposit",
			p,
			&opt,
		)
		if err != nil {
			fmt.Println(err)
		} else {
			res, err := request.Send(cfg.GetAPIAddr() + "/ChangeDeposit")
			if err != nil {
				fmt.Printf("%v\n", ad.Quant)
				fmt.Println("操作失败:", err, string(res))
				return
			}
			fmt.Printf("操作成功，增加抵押成功\n")
		}
	},
}

var AddPoolCmd = &cobra.Command{
	Use:   "change-quota",
	Short: "修改配额",
	Run: func(cmd *cobra.Command, args []string) {
		ad := &struct {
			MinerID    uint64          `json:"miner_id"`
			PoolID     eos.AccountName `json:"pool_id"`
			Minerowner eos.AccountName `json:"minerowner" prompt:"请输入矿机管理员" require:"true"`
			MaxSpace   uint64          `json:"max_space" prompt:"请输入配额 单位：G 最小100G，最大100T" convert:"Block" require:"true"`
		}{
			MinerID: uint64(cfg.IndexID),
			PoolID:  eos.AN(cfg.PoolID),
		}

		info, err := getPoolInfo(cfg.PoolID)
		if err != nil {
			fmt.Println("操作失败:", err)
			return
		}

		p := []eos.PermissionLevel{
			eos.PermissionLevel{
				eos.AN(cfg.Adminacc),
				"active",
			},
			eos.PermissionLevel{
				eos.AN(info[0].PoolOwner),
				"active",
			},
		}
		request, err := transaction.NewSignedTransactionRequest(
			ad,
			"hddpool12345",
			"addm2pool",
			p,
			&opt,
		)
		if err != nil {
			fmt.Println(err)
		} else {
			res, err := request.Send(cfg.GetAPIAddr() + "/changeminerpool")
			if err != nil {
				fmt.Println("操作失败:", err, string(res))
				return
			}
			fmt.Printf("操作成功，修改矿池最大配额成功:%d\n", ad.MaxSpace*16/1024)
		}
	},
}

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "矿机信息",
	Run: func(cmd *cobra.Command, args []string) {
		out, err := api.GetTableRows(eos.GetTableRowsRequest{
			Code:       "hddpool12345",
			Scope:      "hddpool12345",
			Table:      "minerinfo",
			LowerBound: fmt.Sprintf("%d", cfg.IndexID),
			UpperBound: fmt.Sprintf("%d", cfg.IndexID),
			Index:      "1",
			Limit:      1,
			JSON:       true,
			KeyType:    "int64",
		})
		if err != nil {
			fmt.Println("查询失败", err, cfg.IndexID)
		}
		var res []struct {
			MinerID   uint64 `json:"minerid"`
			Owner     string `json:"owner"`
			Admin     string `json:"admin"`
			PoolID    string `json:"pool_id"`
			MaxSpace  uint64 `json:"max_space"`
			SpaceLeft uint64 `json:"space_left"`
		}
		json.Unmarshal(out.Rows, &res)

		for _, v := range res {
			fmt.Println("")
			fmt.Println("------------------矿机信息-----------")
			fmt.Println("矿机ID", v.MinerID)
			fmt.Println("矿机管理员", v.Admin)
			fmt.Println("矿池ID", v.PoolID)
			fmt.Printf("最大采购空间 %d Block(=%dGB)\n", v.MaxSpace, v.MaxSpace*16/1024/1024)
		}
		getDepInfo()
	},
}

func getDepInfo() {
	out, err := api.GetTableRows(eos.GetTableRowsRequest{
		Code:       "hdddeposit12",
		Scope:      "hdddeposit12",
		Table:      "miner2dep",
		LowerBound: fmt.Sprintf("%d", cfg.IndexID),
		UpperBound: fmt.Sprintf("%d", cfg.IndexID),
		Index:      "1",
		Limit:      1,
		JSON:       true,
		KeyType:    "int64",
	})
	if err != nil {
		fmt.Println("查询失败", err, cfg.IndexID)
	}
	var res []struct {
		AccountName string `json:"account_name"`
		Deposit     string `json:"deposit"`
	}
	json.Unmarshal(out.Rows, &res)

	for _, v := range res {
		fmt.Println("------------------抵押信息-----------")
		fmt.Println("抵押账户名", v.AccountName)
		fmt.Println("抵押金额", v.Deposit)
		fmt.Println("____________________________________")
		fmt.Println("")
	}
}

func init() {

	c, err := config.ReadConfig()
	if err == nil {
		cfg = c
		AccountCmd.AddCommand(
			infoCmd,
			changeDepositCmd,
			changeMaxSpaceCmd,
			changeAdminCmd,
			changeOwnerCmd,
			changePoolIDCmd,
			changeDepAccCmd,
		)
		baseNodeUrl = strings.ReplaceAll(cfg.GetAPIAddr(), ":8082", ":8888")
		api = eos.New(baseNodeUrl)
		transaction.Api = api
		opt.FillFromChain(api)
	} else {
		AccountCmd.Short = "账号管理[请先注册]"
	}
}
