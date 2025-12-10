/*
Copyright 2021-2025 Red Hat, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	appstudiov1alpha1 "github.com/konflux-ci/application-api/api/v1alpha1"
	tektonapi "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	tektonapi_v1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	resolution_v1beta1 "github.com/tektoncd/pipeline/pkg/apis/resolution/v1beta1"
	"github.com/tektoncd/pipeline/pkg/client/clientset/versioned/scheme"
	oci "github.com/tektoncd/pipeline/pkg/remote/oci"
	tektonresolvergit "github.com/tektoncd/pipeline/pkg/remoteresolution/resolver/git"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	"github.com/konflux-ci/build-service/pkg/boerrors"
	. "github.com/konflux-ci/build-service/pkg/common"
	gp "github.com/konflux-ci/build-service/pkg/git/gitprovider"
	l "github.com/konflux-ci/build-service/pkg/logs"
)

type BuildPipeline struct {
	Name             string   `json:"name,omitempty"`
	Bundle           string   `json:"bundle,omitempty"`
	GitURL           string   `json:"git-url,omitempty"`
	GitRevision      string   `json:"git-revision,omitempty"`
	GitPath          string   `json:"git-path,omitempty"`
	AdditionalParams []string `json:"additional-params,omitempty"`
}

type pipelineConfig struct {
	DefaultPipelineName string          `json:"default-pipeline-name"`
	Pipelines           []BuildPipeline `json:"pipelines"`
}

// generatePaCPipelineRunConfigs generates PipelineRun YAML configs for given component.
// The generated PipelineRun Yaml content are returned in byte string and in the order of push and pull request.
func (r *ComponentBuildReconciler) generatePaCPipelineRunConfigs(ctx context.Context, component *appstudiov1alpha1.Component, gitClient gp.GitProviderClient, pacTargetBranch string) ([]byte, []byte, error) {
	log := ctrllog.FromContext(ctx)

	var pipelineName string
	var pipelineRef *tektonapi.PipelineRef
	var additionalParams []string
	var err error

	// no need to check error because it would fail already in Reconcile
	pipelineRef, additionalParams, pipelineName, _ = r.GetBuildPipelineFromComponentAnnotation(ctx, component)
	log.Info(fmt.Sprintf("Selected %s pipeline using %q resolver for %s component",
		pipelineName, pipelineRef.Resolver, component.Name),
		l.Audit, "true")

	// Get pipeline from the resolver to be expanded to the PipelineRun
	pipelineSpec, err := r.retrievePipelineSpec(ctx, pipelineRef, pipelineName)
	if err != nil {
		r.EventRecorder.Event(component, "Warning", "ErrorGettingPipeline", err.Error())
		return nil, nil, err
	}

	pipelineRunOnPush, err := generatePaCPipelineRunForComponent(component, pipelineSpec, additionalParams, pacTargetBranch, gitClient, false)
	if err != nil {
		return nil, nil, err
	}
	pipelineRunOnPushYaml, err := yaml.Marshal(pipelineRunOnPush)
	if err != nil {
		return nil, nil, err
	}

	pipelineRunOnPR, err := generatePaCPipelineRunForComponent(component, pipelineSpec, additionalParams, pacTargetBranch, gitClient, true)
	if err != nil {
		return nil, nil, err
	}
	pipelineRunOnPRYaml, err := yaml.Marshal(pipelineRunOnPR)
	if err != nil {
		return nil, nil, err
	}

	return pipelineRunOnPushYaml, pipelineRunOnPRYaml, nil
}

// retrievePipelineSpec retrieves pipeline definition from the given PipelineRef.
func (r *ComponentBuildReconciler) retrievePipelineSpec(ctx context.Context, pipelineRef *tektonapi.PipelineRef, pipelineName string) (*tektonapi.PipelineSpec, error) {
	log := ctrllog.FromContext(ctx)

	var obj runtime.Object

	resolverType := pipelineRef.Resolver
	var pipelineSource string

	switch resolverType {
	case "bundles":
		pipelineBundle, err := getPipelineBundle(pipelineRef)
		if err != nil {
			return nil, err
		}
		pipelineSource = fmt.Sprintf("bundle: %s", pipelineBundle)

		resolver := oci.NewResolver(pipelineBundle, authn.DefaultKeychain)

		if obj, _, err = resolver.Get(ctx, "pipeline", pipelineName); err != nil {
			return nil, err
		}

	case "git":
		gitUrl, gitRevision, gitPathInRepo, err := getGitPipelineParameters(pipelineRef)
		if err != nil {
			return nil, err
		}
		pipelineSource = fmt.Sprintf("git: (%s, %s, %s)", gitUrl, gitRevision, gitPathInRepo)

		gitResolverParams := make(map[string]string, len(pipelineRef.Params))
		for _, param := range pipelineRef.Params {
			gitResolverParams[param.Name] = param.Value.StringVal
		}
		resolver := &tektonresolvergit.Resolver{}

		requestSpec := &resolution_v1beta1.ResolutionRequestSpec{
			Params: pipelineRef.ResolverRef.Params,
		}
		resolved, err := resolver.Resolve(ctx, requestSpec)
		if err != nil {
			return nil, boerrors.NewBuildOpError(
				boerrors.EPipelineRetrievalFailed,
				fmt.Errorf("failed to fetch pipeline %s from source %s", pipelineName, pipelineSource),
			)
		}

		if obj, _, err = scheme.Codecs.UniversalDeserializer().Decode(resolved.Data(), nil, nil); err != nil {
			return nil, err
		}

	default:
		return nil, boerrors.NewBuildOpError(
			boerrors.EUnsupportedPipelineRef,
			fmt.Errorf("unsupported Tekton resolver %q", resolverType),
		)
	}

	var pipelineSpec tektonapi.PipelineSpec

	if v1beta1Pipeline, ok := obj.(tektonapi_v1beta1.PipelineObject); ok {
		v1beta1PipelineSpec := v1beta1Pipeline.PipelineSpec()
		log.Info("Converting from v1beta1 to v1", "PipelineName", pipelineName, "Resolver", resolverType, "PipelineSource", pipelineSource)
		err := v1beta1PipelineSpec.ConvertTo(ctx, &pipelineSpec, &metav1.ObjectMeta{})
		if err != nil {
			return nil, boerrors.NewBuildOpError(
				boerrors.EPipelineConversionFailed,
				fmt.Errorf("pipeline %s from %s: failed to convert from v1beta1 to v1: %w", pipelineName, pipelineSource, err),
			)
		}
	} else if v1Pipeline, ok := obj.(*tektonapi.Pipeline); ok {
		pipelineSpec = v1Pipeline.PipelineSpec()
	} else {
		return nil, boerrors.NewBuildOpError(
			boerrors.EPipelineRetrievalFailed,
			fmt.Errorf("failed to extract pipeline %s from source %s", pipelineName, pipelineSource),
		)
	}

	return &pipelineSpec, nil
}

// GetBuildPipelineFromComponentAnnotation parses pipeline annotation on component and returns build pipeline
func (r *ComponentBuildReconciler) GetBuildPipelineFromComponentAnnotation(ctx context.Context, component *appstudiov1alpha1.Component) (*tektonapi.PipelineRef, []string, string, error) {
	buildPipeline, err := readBuildPipelineAnnotation(component)
	if err != nil {
		return nil, nil, "", err
	}
	if buildPipeline == nil {
		err := fmt.Errorf("missing or empty pipeline annotation: %s, will add default one to the component", component.Annotations[defaultBuildPipelineAnnotation])
		return nil, nil, "", boerrors.NewBuildOpError(boerrors.EMissingPipelineAnnotation, err)
	}
	if buildPipeline.Name == "" {
		err = fmt.Errorf("missing name in pipeline annotation: name=%s", buildPipeline.Name)
		return nil, nil, "", boerrors.NewBuildOpError(boerrors.EWrongPipelineAnnotation, err)
	}
	additionalParams := []string{}

	pipelinesConfigMap := &corev1.ConfigMap{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: buildPipelineConfigMapResourceName, Namespace: BuildServiceNamespaceName}, pipelinesConfigMap); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil, "", boerrors.NewBuildOpError(boerrors.EBuildPipelineConfigNotDefined, err)
		}
		return nil, nil, "", err
	}

	buildPipelineData := &pipelineConfig{}
	if err := yaml.Unmarshal([]byte(pipelinesConfigMap.Data[buildPipelineConfigName]), buildPipelineData); err != nil {
		return nil, nil, "", boerrors.NewBuildOpError(boerrors.EBuildPipelineConfigNotValid, err)
	}

	pipelineUsesBundlesResolver := buildPipeline.Bundle != ""
	pipelineUsesGitResolver := buildPipeline.GitURL != "" || buildPipeline.GitRevision != "" || buildPipeline.GitPath != ""

	if pipelineUsesGitResolver && pipelineUsesBundlesResolver {
		err = fmt.Errorf("cannot specify multiple resolvers at the same time")
		return nil, nil, "", boerrors.NewBuildOpError(boerrors.EWrongPipelineAnnotation, err)
	}

	// look for the pipeline by name in ConfigMap
	var configMapPipeline *BuildPipeline
	for i, pipeline := range buildPipelineData.Pipelines {
		if pipeline.Name == buildPipeline.Name {
			configMapPipeline = &buildPipelineData.Pipelines[i]
			additionalParams = pipeline.AdditionalParams
			break
		}
	}

	var resolverType tektonapi.ResolverName
	if pipelineUsesBundlesResolver {
		resolverType = "bundles"
	} else if pipelineUsesGitResolver {
		resolverType = "git"
	} else if configMapPipeline != nil {
		// infer resolver from ConfigMap
		if configMapPipeline.Bundle != "" {
			resolverType = "bundles"
		} else if configMapPipeline.GitURL != "" || configMapPipeline.GitRevision != "" || configMapPipeline.GitPath != "" {
			resolverType = "git"
		}
	}

	if resolverType == "" {
		err = fmt.Errorf("cannot determine resolver for pipeline: name=%s", buildPipeline.Name)
		return nil, nil, "", boerrors.NewBuildOpError(boerrors.EBuildPipelineInvalid, err)
	}

	switch resolverType {
	case "bundles":
		finalBundle := buildPipeline.Bundle

		if finalBundle == "" || finalBundle == "latest" {
			// requested pipeline was not found in configMap
			if configMapPipeline == nil || configMapPipeline.Bundle == "" {
				err = fmt.Errorf("cannot find pipeline in config map: name=%s, bundle=%s", buildPipeline.Name, finalBundle)
				return nil, nil, "", boerrors.NewBuildOpError(boerrors.EBuildPipelineInvalid, err)
			}
			finalBundle = configMapPipeline.Bundle
		}

		pipelineRef := &tektonapi.PipelineRef{
			ResolverRef: tektonapi.ResolverRef{
				Resolver: resolverType,
				Params: []tektonapi.Param{
					{Name: "name", Value: *tektonapi.NewStructuredValues(buildPipeline.Name)},
					{Name: "bundle", Value: *tektonapi.NewStructuredValues(finalBundle)},
					{Name: "kind", Value: *tektonapi.NewStructuredValues("pipeline")},
				},
			},
		}
		return pipelineRef, additionalParams, buildPipeline.Name, nil

	case "git":
		finalGitURL := buildPipeline.GitURL
		finalGitRevision := buildPipeline.GitRevision
		finalGitPath := buildPipeline.GitPath

		// get the remaining parameters from configMap
		if configMapPipeline != nil {
			if finalGitURL == "" {
				finalGitURL = configMapPipeline.GitURL
			}
			if finalGitRevision == "" {
				finalGitRevision = configMapPipeline.GitRevision
			}
			if finalGitPath == "" {
				finalGitPath = configMapPipeline.GitPath
			}
		}

		// requested pipeline was not found in configMap
		if finalGitURL == "" || finalGitRevision == "" || finalGitPath == "" {
			err = fmt.Errorf("incomplete git resolver parameters for pipeline: name=%s, git-url=%s, git-revision=%s, git-path=%s", buildPipeline.Name, finalGitURL, finalGitRevision, finalGitPath)
			return nil, nil, "", boerrors.NewBuildOpError(boerrors.EBuildPipelineInvalid, err)
		}

		pipelineRef := &tektonapi.PipelineRef{
			ResolverRef: tektonapi.ResolverRef{
				Resolver: resolverType,
				Params: []tektonapi.Param{
					{Name: "url", Value: *tektonapi.NewStructuredValues(finalGitURL)},
					{Name: "revision", Value: *tektonapi.NewStructuredValues(finalGitRevision)},
					{Name: "pathInRepo", Value: *tektonapi.NewStructuredValues(finalGitPath)},
				},
			},
		}
		return pipelineRef, additionalParams, buildPipeline.Name, nil
	}

	return nil, nil, "", boerrors.NewBuildOpError(
		boerrors.EUnsupportedPipelineRef,
		fmt.Errorf("unsupported Tekton resolver %q", resolverType),
	)
}

func readBuildPipelineAnnotation(component *appstudiov1alpha1.Component) (*BuildPipeline, error) {
	if component.Annotations == nil {
		return nil, nil
	}

	requestedPipeline, requestedPipelineExists := component.Annotations[defaultBuildPipelineAnnotation]
	if requestedPipelineExists && requestedPipeline != "" {
		buildPipeline := &BuildPipeline{}
		buildPipelineBytes := []byte(requestedPipeline)

		if err := json.Unmarshal(buildPipelineBytes, buildPipeline); err != nil {
			return nil, boerrors.NewBuildOpError(boerrors.EFailedToParsePipelineAnnotation, err)
		}
		return buildPipeline, nil
	}
	return nil, nil
}

// SetDefaultBuildPipelineComponentAnnotation sets default build pipeline to component pipeline annotation
func (r *ComponentBuildReconciler) SetDefaultBuildPipelineComponentAnnotation(ctx context.Context, component *appstudiov1alpha1.Component) error {
	log := ctrllog.FromContext(ctx)
	pipelinesConfigMap := &corev1.ConfigMap{}

	if err := r.Client.Get(ctx, types.NamespacedName{Name: buildPipelineConfigMapResourceName, Namespace: BuildServiceNamespaceName}, pipelinesConfigMap); err != nil {
		if errors.IsNotFound(err) {
			return boerrors.NewBuildOpError(boerrors.EBuildPipelineConfigNotDefined, err)
		}
		return err
	}

	buildPipelineData := &pipelineConfig{}
	if err := yaml.Unmarshal([]byte(pipelinesConfigMap.Data[buildPipelineConfigName]), buildPipelineData); err != nil {
		return boerrors.NewBuildOpError(boerrors.EBuildPipelineConfigNotValid, err)
	}

	pipelineAnnotation := fmt.Sprintf("{\"name\":\"%s\"}", buildPipelineData.DefaultPipelineName)
	if component.Annotations == nil {
		component.Annotations = make(map[string]string)
	}
	component.Annotations[defaultBuildPipelineAnnotation] = pipelineAnnotation

	if err := r.Client.Update(ctx, component); err != nil {
		log.Error(err, fmt.Sprintf("failed to update component with default pipeline annotation %s", defaultBuildPipelineAnnotation))
		return err
	}
	log.Info(fmt.Sprintf("updated component with default pipeline annotation %s", defaultBuildPipelineAnnotation))
	return nil
}

// generatePaCPipelineRunForComponent returns pipeline run definition to build component source with.
// Generated pipeline run contains placeholders that are expanded by Pipeline-as-Code.
func generatePaCPipelineRunForComponent(
	component *appstudiov1alpha1.Component,
	pipelineSpec *tektonapi.PipelineSpec,
	additionalParams []string,
	pacTargetBranch string,
	gitClient gp.GitProviderClient,
	onPull bool) (*tektonapi.PipelineRun, error) {

	if pacTargetBranch == "" {
		return nil, fmt.Errorf("target branch can't be empty for generating PaC PipelineRun for: %v", component)
	}
	pipelineCelExpression, err := generateCelExpressionForPipeline(component, gitClient, pacTargetBranch, onPull)
	if err != nil {
		return nil, fmt.Errorf("failed to generate cel expression for pipeline: %w", err)
	}
	repoUrl := getGitRepoUrl(*component)

	annotations := map[string]string{
		"pipelinesascode.tekton.dev/cancel-in-progress": "false",
		"pipelinesascode.tekton.dev/max-keep-runs":      "3",
		"build.appstudio.redhat.com/target_branch":      "{{target_branch}}",
		pacCelExpressionAnnotationName:                  pipelineCelExpression,
		gitCommitShaAnnotationName:                      "{{revision}}",
		gitRepoAtShaAnnotationName:                      gitClient.GetBrowseRepositoryAtShaLink(repoUrl, "{{revision}}"),
	}
	labels := map[string]string{
		ApplicationNameLabelName:                component.Spec.Application,
		ComponentNameLabelName:                  component.Name,
		"pipelines.appstudio.openshift.io/type": "build",
	}

	imageRepo := getContainerImageRepositoryForComponent(component)

	var pipelineName string
	var proposedImage string
	if onPull {
		annotations["pipelinesascode.tekton.dev/cancel-in-progress"] = "true"
		annotations["build.appstudio.redhat.com/pull_request_number"] = "{{pull_request_number}}"
		pipelineName = component.Name + pipelineRunOnPRSuffix
		proposedImage = imageRepo + ":on-pr-{{revision}}"
	} else {
		pipelineName = component.Name + pipelineRunOnPushSuffix
		proposedImage = imageRepo + ":{{revision}}"
	}

	params := []tektonapi.Param{
		{Name: "git-url", Value: tektonapi.ParamValue{Type: "string", StringVal: "{{source_url}}"}},
		{Name: "revision", Value: tektonapi.ParamValue{Type: "string", StringVal: "{{revision}}"}},
		{Name: "output-image", Value: tektonapi.ParamValue{Type: "string", StringVal: proposedImage}},
	}
	if onPull {
		prImageExpiration := os.Getenv(PipelineRunOnPRExpirationEnvVar)
		if prImageExpiration == "" {
			prImageExpiration = PipelineRunOnPRExpirationDefault
		}
		params = append(params, tektonapi.Param{Name: "image-expires-after", Value: tektonapi.ParamValue{Type: "string", StringVal: prImageExpiration}})
	}

	for _, additionalParam := range additionalParams {
		for _, pipelineParam := range pipelineSpec.Params {
			if additionalParam == pipelineParam.Name {
				if pipelineParam.Type == "string" {
					params = append(params, tektonapi.Param{Name: additionalParam, Value: tektonapi.ParamValue{Type: "string", StringVal: pipelineParam.Default.StringVal}})
					break
				}
				if pipelineParam.Type == "array" {
					params = append(params, tektonapi.Param{Name: additionalParam, Value: tektonapi.ParamValue{Type: "array", ArrayVal: pipelineParam.Default.ArrayVal}})
					break
				}
				if pipelineParam.Type == "object" {
					params = append(params, tektonapi.Param{Name: additionalParam, Value: tektonapi.ParamValue{Type: "object", ObjectVal: pipelineParam.Default.ObjectVal}})
					break
				}
			}
		}
	}

	if component.Spec.Source.GitSource.DockerfileURL != "" {
		params = append(params, tektonapi.Param{Name: "dockerfile", Value: tektonapi.ParamValue{Type: "string", StringVal: component.Spec.Source.GitSource.DockerfileURL}})
	} else {
		params = append(params, tektonapi.Param{Name: "dockerfile", Value: tektonapi.ParamValue{Type: "string", StringVal: "Dockerfile"}})
	}
	pathContext := getPathContext(component.Spec.Source.GitSource.Context, "")
	if pathContext != "" {
		params = append(params, tektonapi.Param{Name: "path-context", Value: tektonapi.ParamValue{Type: "string", StringVal: pathContext}})
	}

	pipelineRunWorkspaces := createWorkspaceBinding(pipelineSpec.Workspaces)

	pipelineRun := &tektonapi.PipelineRun{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PipelineRun",
			APIVersion: "tekton.dev/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        pipelineName,
			Namespace:   component.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: tektonapi.PipelineRunSpec{
			PipelineSpec: pipelineSpec,
			Params:       params,
			Workspaces:   pipelineRunWorkspaces,
			TaskRunTemplate: tektonapi.PipelineTaskRunTemplate{
				ServiceAccountName: getBuildPipelineServiceAccountName(component),
			},
		},
	}

	return pipelineRun, nil
}

func getPathContext(gitContext, dockerfileContext string) string {
	if gitContext == "" && dockerfileContext == "" {
		return ""
	}
	separator := string(filepath.Separator)
	path := filepath.Join(gitContext, dockerfileContext)
	path = filepath.Clean(path)
	path = strings.TrimPrefix(path, separator)
	return path
}

func createWorkspaceBinding(pipelineWorkspaces []tektonapi.PipelineWorkspaceDeclaration) []tektonapi.WorkspaceBinding {
	pipelineRunWorkspaces := []tektonapi.WorkspaceBinding{}
	for _, workspace := range pipelineWorkspaces {
		switch workspace.Name {
		case "workspace":
			pipelineRunWorkspaces = append(pipelineRunWorkspaces,
				tektonapi.WorkspaceBinding{
					Name:                workspace.Name,
					VolumeClaimTemplate: generateVolumeClaimTemplate(),
				})
		case "git-auth":
			pipelineRunWorkspaces = append(pipelineRunWorkspaces,
				tektonapi.WorkspaceBinding{
					Name:   workspace.Name,
					Secret: &corev1.SecretVolumeSource{SecretName: "{{ git_auth_secret }}"},
				})
		}
	}
	return pipelineRunWorkspaces
}

func generateVolumeClaimTemplate() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				"ReadWriteOnce",
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					"storage": resource.MustParse("1Gi"),
				},
			},
		},
	}
}

// generateCelExpressionForPipeline generates value for pipelinesascode.tekton.dev/on-cel-expression annotation
// in order to have better flexibility with git events filtering.
// Examples of returned values:
// event == "push" && target_branch == "main"
// event == "pull_request" && target_branch == "my-branch" && ( "component-src-dir/***".pathChanged() || ".tekton/pipeline.yaml".pathChanged() || "dockerfiles/my-component/Dockerfile".pathChanged() )
func generateCelExpressionForPipeline(component *appstudiov1alpha1.Component, gitClient gp.GitProviderClient, targetBranch string, onPull bool) (string, error) {
	eventType := "push"
	if onPull {
		eventType = "pull_request"
	}
	eventCondition := fmt.Sprintf(`event == "%s"`, eventType)

	targetBranchCondition := fmt.Sprintf(`target_branch == "%s"`, targetBranch)
	repoUrl := getGitRepoUrl(*component)

	// Set path changed event filtering only for Components that are stored within a directory of the git repository.
	pathChangedSuffix := ""
	if component.Spec.Source.GitSource.Context != "" && component.Spec.Source.GitSource.Context != "/" && component.Spec.Source.GitSource.Context != "./" && component.Spec.Source.GitSource.Context != "." {
		contextDir := component.Spec.Source.GitSource.Context
		if !strings.HasSuffix(contextDir, "/") {
			contextDir += "/"
		}

		// If a Dockerfile is defined for the Component,
		// we should rebuild the Component if the Dockerfile has been changed.
		dockerfilePathChangedSuffix := ""
		dockerfile := component.Spec.Source.GitSource.DockerfileURL
		if dockerfile != "" {
			// Ignore dockerfile that is not stored in the same git repository but downloaded by an URL.
			if !strings.Contains(dockerfile, "://") {
				// dockerfile could be relative to the context directory or repository root.
				// To avoid unessesary builds, it's required to pass absolute path to the Dockerfile.
				branch := component.Spec.Source.GitSource.Revision
				dockerfilePath := contextDir + dockerfile
				isDockerfileInContextDir, err := gitClient.IsFileExist(repoUrl, branch, dockerfilePath)
				if err != nil {
					return "", err
				}
				// If the Dockerfile is inside context directory, no changes to event filter needed.
				if !isDockerfileInContextDir {
					// Pipelines as Code doesn't match path if it starts from /
					dockerfileAbsolutePath := strings.TrimPrefix(dockerfile, "/")
					dockerfilePathChangedSuffix = fmt.Sprintf(`|| "%s".pathChanged() `, dockerfileAbsolutePath)
				}
			}
		}

		pipelineFileName := component.Name + "-" + pipelineRunOnPushFilename
		if onPull {
			pipelineFileName = component.Name + "-" + pipelineRunOnPRFilename
		}

		pathChangedSuffix = fmt.Sprintf(` && ( "%s***".pathChanged() || ".tekton/%s".pathChanged() %s)`, contextDir, pipelineFileName, dockerfilePathChangedSuffix)
	}

	return fmt.Sprintf("%s && %s%s", eventCondition, targetBranchCondition, pathChangedSuffix), nil
}

func getContainerImageRepositoryForComponent(component *appstudiov1alpha1.Component) string {
	if component.Spec.ContainerImage != "" {
		return getContainerImageRepository(component.Spec.ContainerImage)
	}
	return ""
}

// getContainerImageRepository removes tag or SHA has from container image reference
func getContainerImageRepository(image string) string {
	if strings.Contains(image, "@") {
		// registry.io/user/image@sha256:586ab...d59a
		return strings.Split(image, "@")[0]
	}
	// registry.io/user/image:tag
	return strings.Split(image, ":")[0]
}

func getPipelineBundle(pipelineRef *tektonapi.PipelineRef) (string, error) {
	if pipelineRef.Resolver != "" && pipelineRef.Resolver != "bundles" {
		return "", boerrors.NewBuildOpError(
			boerrors.EUnsupportedPipelineRef,
			fmt.Errorf("unsupported Tekton resolver %q", pipelineRef.Resolver),
		)
	}

	var bundle string

	for _, param := range pipelineRef.Params {
		switch param.Name {
		case "bundle":
			bundle = param.Value.StringVal
		}
	}

	if bundle == "" {
		return "", boerrors.NewBuildOpError(
			boerrors.EMissingParamsForBundleResolver,
			fmt.Errorf("missing bundle in pipelineRef: bundle=%s", bundle),
		)
	}

	return bundle, nil
}

func getGitPipelineParameters(pipelineRef *tektonapi.PipelineRef) (string, string, string, error) {
	if pipelineRef.Resolver != "" && pipelineRef.Resolver != "git" {
		return "", "", "", boerrors.NewBuildOpError(
			boerrors.EUnsupportedPipelineRef,
			fmt.Errorf("unsupported Tekton resolver %q", pipelineRef.Resolver),
		)
	}

	var gitUrl string
	var gitRevision string
	var gitPathInRepo string

	for _, param := range pipelineRef.Params {
		switch param.Name {
		case "url":
			gitUrl = param.Value.StringVal
		case "revision":
			gitRevision = param.Value.StringVal
		case "pathInRepo":
			gitPathInRepo = param.Value.StringVal
		}
	}

	if gitUrl == "" || gitRevision == "" || gitPathInRepo == "" {
		return "", "", "", boerrors.NewBuildOpError(
			boerrors.EMissingParamsForGitResolver,
			fmt.Errorf("missing git resolver param in pipelineRef: url=%s, revision=%s, pathInRepo=%s", gitUrl, gitRevision, gitPathInRepo),
		)
	}

	return gitUrl, gitRevision, gitPathInRepo, nil
}
