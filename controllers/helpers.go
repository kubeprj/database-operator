package controllers

import (
	"context"
	"errors"
	"hash/crc32"
	"strings"

	"github.com/kubeprj/database-operator/accountsvr"
	v1 "github.com/kubeprj/database-operator/api/v1"
	"github.com/oklog/ulid/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func newDatabaseAccountName(ctx context.Context) v1.PostgreSQLResourceName {
	return v1.PostgreSQLResourceName(strings.ToLower("k8s_" + ulid.Make().String()))
}

func secretRun(ctx context.Context, r client.Reader, w client.Writer, accountSvr *accountsvr.Server, dbAccount *v1.DatabaseAccount, f func(secret *corev1.Secret) error) error {
	logger := log.FromContext(ctx)

	secret, err := secretGet(ctx, r, dbAccount)
	if errors.Is(err, ErrNewSecret) {
		accountSvr.CopyConfigToSecret(secret)
		if err := w.Create(ctx, secret); err != nil {
			logger.Error(err, "unable to create secret")

			return err
		}
	} else if err != nil {
		return err
	}

	preChecksum := crc32.ChecksumIEEE([]byte(secret.String()))

	if err := f(secret); err != nil {
		return err
	}

	postChecksum := crc32.ChecksumIEEE([]byte(secret.String()))

	// logger.V(1).Info("Checksum Comparison", "preChecksum", preChecksum, "postChecksum", postChecksum)

	if preChecksum != postChecksum {
		logger.V(1).Info("Secret updated", "preChecksum", preChecksum, "postChecksum", postChecksum)
		if err := w.Update(ctx, secret); err != nil {
			logger.Error(err, "unable to update secret")

			return err
		}
	}

	return nil
}

func secretGet(ctx context.Context, r client.Reader, dbAccount *v1.DatabaseAccount) (*corev1.Secret, error) {
	secret := &corev1.Secret{}

	if err := r.Get(ctx, dbAccount.GetSecretName(), secret); apierrors.IsNotFound(err) {

		// user := uuid.NewUUID()
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:        dbAccount.GetSecretName().Name,
				Namespace:   dbAccount.Namespace,
				Annotations: dbAccount.Spec.SecretTemplate.Annotations,
				Labels:      dbAccount.Spec.SecretTemplate.Labels,
			},
			StringData: map[string]string{},
			Data:       map[string][]byte{},
			Type:       `kubeprj.github.io/database-account`,
		}
		if dbAccount.Spec.OnDelete == v1.OnDeleteDelete {
			secretAddOwnerRefs(secret, dbAccount)
		}

		return secret, ErrNewSecret
	} else if err != nil {
		return secret, err
	}

	return secret, nil
}

func secretAddOwnerRefs(secret *corev1.Secret, dbAccount *v1.DatabaseAccount) {
	secret.OwnerReferences = []metav1.OwnerReference{*dbAccount.GetReference()}
}
