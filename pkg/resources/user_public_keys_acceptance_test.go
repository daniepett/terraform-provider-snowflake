package resources_test

import (
	"bytes"
	"strings"
	"testing"
	"text/template"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/testhelpers"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/stretchr/testify/require"
)

func TestAcc_UserPublicKeys(t *testing.T) {
	r := require.New(t)
	prefix := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))
	sshkey1, err := testhelpers.Fixture("userkey1")
	r.NoError(err)
	sshkey2, err := testhelpers.Fixture("userkey2")
	r.NoError(err)

	resource.ParallelTest(t, resource.TestCase{
		Providers: providers(),
		Steps: []resource.TestStep{
			{
				Config: uPublicKeysConfig(r, PublicKeyData{
					Prefix:     prefix,
					PublicKey1: sshkey1,
					PublicKey2: sshkey2,
				}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_user.w", "name", prefix),

					resource.TestCheckResourceAttr("snowflake_user_public_keys.foobar", "rsa_public_key", sshkey1),
					resource.TestCheckResourceAttr("snowflake_user_public_keys.foobar", "rsa_public_key_2", sshkey2),
				),
			},
			// IMPORT
			{
				ResourceName:            "snowflake_user.w",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"password", "rsa_public_key", "rsa_public_key_2", "must_change_password"},
			},
		},
	})
}

type PublicKeyData struct {
	Prefix     string
	PublicKey1 string
	PublicKey2 string
}

func uPublicKeysConfig(r *require.Assertions, data PublicKeyData) string {
	t := `
resource "snowflake_user" "w" {
	name = "{{.Prefix}}"
	comment = "test comment"
	login_name = "{{.Prefix}}_login"
	display_name = "Display Name"
	first_name = "Marcin"
	last_name = "Zukowski"
	email = "fake@email.com"
	disabled = false
	default_warehouse="foo"
	default_role="foo"
	default_namespace="foo"
}

resource "snowflake_user_public_keys" "foobar" {
	name = snowflake_user.w.name
	rsa_public_key = <<KEY
{{ .PublicKey1 }}
	KEY

	rsa_public_key_2 = <<KEY
{{ .PublicKey2 }}
	KEY
}
`
	conf := bytes.NewBuffer(nil)
	err := template.Must(template.New("user").Parse(t)).Execute(conf, data)
	r.NoError(err)
	return conf.String()
}
